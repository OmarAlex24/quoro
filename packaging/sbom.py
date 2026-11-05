#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Build the software bill of materials of a Quoro release.

Two subcommands:

  components  walks the cargo metadata of the release, the panel's npm
              lockfile and the pinned artifacts, and writes the component
              list as JSON. The Rust side reads the same list to compile it
              into the binary, so the format is a contract.
  document    turns a component list into a CycloneDX 1.6 JSON document.

Only the Python standard library is used, because the script runs in the
release build image. Every part of the output is sorted and the only
timestamp comes from SOURCE_DATE_EPOCH, so the same inputs give the same
bytes.
"""

import argparse
import hashlib
import json
import re
import sys
import urllib.parse
from datetime import datetime, timezone

# The package names whose dependencies make up the release.
ROOTS = ("quoro", "quoro-init")

# Pin names and the CycloneDX component type each one gets.
PIN_TYPES = {
    "cloud-hypervisor": "application",
    "cloudflared": "application",
    "guest-kernel": "operating-system",
    "buildkit": "container",
}

PIN_FIELDS = 6  # name version arch file url sha256
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
BUILDKIT_RE = re.compile(
    r"^(?P<registry>[a-z0-9.-]+)/(?P<repository>[^:@\s]+):(?P<tag>[^@\s]+)"
    r"@sha256:(?P<digest>[0-9a-f]{64})$"
)


def fail(message):
    """Print an error and exit with status 1."""
    sys.stderr.write("error: %s\n" % message)
    raise SystemExit(1)


def read_json(path):
    """Read a JSON file, failing with the path on any problem."""
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except OSError as error:
        fail("cannot read %s: %s" % (path, error))
    except ValueError as error:
        fail("%s is not valid JSON: %s" % (path, error))


def write_json(path, value):
    """Write a JSON file in the one serialization both outputs share."""
    text = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    try:
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
    except OSError as error:
        fail("cannot write %s: %s" % (path, error))


def quote_purl(text):
    """Percent-encode one purl path segment."""
    return urllib.parse.quote(text, safe="")


def is_proc_macro(package):
    """True when every target of the package is a proc-macro.

    Such a package runs at compile time and its code is not part of the
    binary, so it is not a component of what the release ships.
    """
    targets = package.get("targets") or []
    return bool(targets) and all("proc-macro" in (t.get("kind") or ()) for t in targets)


def crate_source(source):
    """Map a cargo package source to the component's `source` field."""
    if source is None:
        return "path"
    if source.startswith("git+"):
        return "git"
    if source.startswith("registry+") or source.startswith("sparse+"):
        if "crates.io" in source:
            return "crates.io"
        fail("unsupported crate registry: %s" % source)
    fail("unsupported crate source: %s" % source)


def crate_components(metadata):
    """Return the crates compiled into the root binaries, as components."""
    packages = {package["id"]: package for package in metadata.get("packages", [])}
    nodes = {node["id"]: node for node in (metadata.get("resolve") or {}).get("nodes", [])}

    roots = [package["id"] for package in packages.values() if package["name"] in ROOTS]
    missing = set(ROOTS) - {packages[root]["name"] for root in roots}
    if missing:
        fail("cargo metadata has no package named %s" % ", ".join(sorted(missing)))
    for root in roots:
        if root not in nodes:
            fail("cargo metadata has no resolve node for %s" % packages[root]["name"])

    seen = set()
    queue = list(roots)
    while queue:
        package_id = queue.pop(0)
        if package_id in seen:
            continue
        seen.add(package_id)
        node = nodes.get(package_id)
        if node is None:
            fail("cargo metadata has no resolve node for %s" % packages[package_id]["name"])
        for dep in node.get("deps") or []:
            kinds = [kind.get("kind") for kind in dep.get("dep_kinds") or []]
            # Only a normal dependency is compiled into the binary; build and
            # dev dependencies are not part of the release.
            if None in kinds:
                target = dep["pkg"]
                if target not in packages:
                    fail("cargo metadata has no package for %s" % target)
                if is_proc_macro(packages[target]):
                    continue
                queue.append(target)

    components = []
    for package_id in seen:
        package = packages[package_id]
        component = {
            "kind": "crate",
            "name": package["name"],
            "version": package["version"],
            "purl": "pkg:cargo/%s@%s"
            % (quote_purl(package["name"]), quote_purl(package["version"])),
            "source": crate_source(package.get("source")),
        }
        if package.get("license"):
            component["license"] = package["license"]
        components.append(component)
    return components


def npm_components(lockfile):
    """Return the production npm packages of the panel, as components."""
    packages = lockfile.get("packages")
    if not isinstance(packages, dict):
        fail("the lockfile has no packages object (lockfileVersion 2 or 3 is required)")

    components = []
    seen = set()
    for key, entry in packages.items():
        if not key.startswith("node_modules/"):
            continue
        if entry.get("dev") or entry.get("devOptional"):
            continue
        name = key.rsplit("node_modules/", 1)[-1]
        version = entry.get("version")
        if not name or not version:
            fail("lockfile entry %s has no name or version" % key)
        if (name, version) in seen:
            continue
        seen.add((name, version))
        component = {
            "kind": "npm",
            "name": name,
            "version": version,
            "purl": "pkg:npm/%s@%s" % (quote_purl(name).replace("%2F", "/"), quote_purl(version)),
        }
        if entry.get("license"):
            component["license"] = entry["license"]
        components.append(component)
    return components


def read_pin_lines(path):
    """Yield the data lines of a pins.txt file, with comments skipped."""
    try:
        with open(path, encoding="utf-8") as handle:
            lines = handle.read().splitlines()
    except OSError as error:
        fail("cannot read %s: %s" % (path, error))

    for number, line in enumerate(lines, 1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        yield number, stripped.split()


def pin_components(path):
    """Return the pinned artifacts of one `name version arch file url sha256` file."""
    components = []
    for number, fields in read_pin_lines(path):
        if len(fields) != PIN_FIELDS:
            fail(
                "%s:%d: expected `name version arch file url sha256`, got %d fields"
                % (path, number, len(fields))
            )
        name, version, arch, _file, url, sha256 = fields
        if not SHA256_RE.match(sha256):
            fail("%s:%d: %s is not a sha256 digest" % (path, number, sha256))
        purl = "pkg:generic/%s@%s?arch=%s&download_url=%s" % (
            quote_purl(name),
            quote_purl(version),
            quote_purl(arch),
            quote_purl(url),
        )
        components.append(
            {
                "kind": "pin",
                "name": name,
                "version": version,
                "arch": arch,
                "purl": purl,
                "sha256": sha256,
            }
        )
    return components


def buildkit_components(path):
    """Return the pinned BuildKit image as a component."""
    components = []
    for number, fields in read_pin_lines(path):
        if len(fields) != 3 or fields[0] != "buildkit-image":
            fail("%s:%d: expected `buildkit-image <version> <reference>`" % (path, number))
        _name, version, reference = fields
        match = BUILDKIT_RE.match(reference)
        if not match:
            fail(
                "%s:%d: expected docker.io/<repository>:<tag>@sha256:<digest>, got %s"
                % (path, number, reference)
            )
        digest = match.group("digest")
        purl = "pkg:docker/%s@sha256%%3A%s?repository_url=%s&tag=%s" % (
            quote_purl(match.group("repository")).replace("%2F", "/"),
            digest,
            quote_purl(match.group("registry")),
            quote_purl(match.group("tag")),
        )
        components.append(
            {
                "kind": "pin",
                "name": "buildkit",
                "version": version,
                "purl": purl,
                "sha256": digest,
            }
        )
    return components


def sort_key(component):
    """The one component order, so the list is byte-stable."""
    return (
        component["kind"],
        component["name"],
        component["version"],
        component.get("arch", ""),
    )


def command_components(args):
    """Write the component list JSON."""
    components = []
    components.extend(crate_components(read_json(args.metadata)))
    components.extend(npm_components(read_json(args.lockfile)))
    for path in args.pins:
        components.extend(pin_components(path))
    components.extend(buildkit_components(args.buildkit))

    components.sort(key=sort_key)
    write_json(args.out, {"schema": 1, "components": components})
    kinds = {}
    for component in components:
        kinds[component["kind"]] = kinds.get(component["kind"], 0) + 1
    print(
        "wrote %s: %d components (%s)"
        % (
            args.out,
            len(components),
            ", ".join("%s %d" % (kind, kinds[kind]) for kind in sorted(kinds)),
        ),
        file=sys.stderr,
    )
    return 0


def sha256_file(path):
    """Return the hex sha256 of a file."""
    digest = hashlib.sha256()
    try:
        with open(path, "rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as error:
        fail("cannot read %s: %s" % (path, error))
    return digest.hexdigest()


def pin_type(name):
    """The CycloneDX type of a pinned artifact."""
    if name not in PIN_TYPES:
        fail("no CycloneDX type is defined for the pin %s" % name)
    return PIN_TYPES[name]


def pin_url(purl):
    """The download URL a pin purl carries, if any."""
    query = purl.partition("?")[2]
    for key, value in urllib.parse.parse_qsl(query):
        if key == "download_url":
            return value
    return None


def document_component(component):
    """Map one component of the list to a CycloneDX component."""
    entry = {
        "type": "library",
        "bom-ref": component["purl"],
        "name": component["name"],
        "version": component["version"],
        "purl": component["purl"],
    }
    kind = component["kind"]
    if kind == "pin":
        entry["type"] = pin_type(component["name"])
        entry["hashes"] = [{"alg": "SHA-256", "content": component["sha256"]}]
        url = pin_url(component["purl"])
        if url:
            entry["externalReferences"] = [{"type": "distribution", "url": url}]
    elif kind in ("crate", "npm"):
        if component.get("license"):
            entry["licenses"] = [{"expression": component["license"]}]
    else:
        fail("unknown component kind: %s" % kind)
    return entry


def command_document(args):
    """Write the CycloneDX 1.6 document."""
    if args.source_date_epoch < 0:
        fail("SOURCE_DATE_EPOCH must be a non-negative integer")
    data = read_json(args.components)
    if data.get("schema") != 1:
        fail("%s is not a schema 1 component list" % args.components)
    components = data.get("components")
    if not isinstance(components, list):
        fail("%s has no components list" % args.components)
    if not re.match(r"^[A-Za-z0-9.+_-]+$", args.version):
        fail("the version contains characters that are unsafe: %s" % args.version)

    components.sort(key=sort_key)
    timestamp = datetime.fromtimestamp(args.source_date_epoch, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    document = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.6",
        "version": 1,
        "metadata": {
            "timestamp": timestamp,
            "component": {
                "type": "application",
                "bom-ref": "quoro@%s" % args.version,
                "name": "quoro",
                "version": args.version,
                "hashes": [{"alg": "SHA-256", "content": sha256_file(args.tarball)}],
                "purl": "pkg:generic/quoro@%s" % quote_purl(args.version),
            },
        },
        "components": [document_component(component) for component in components],
    }
    write_json(args.out, document)
    print(
        "wrote %s: %d components at %s" % (args.out, len(components), timestamp),
        file=sys.stderr,
    )
    return 0


def main(argv):
    """Parse the command line and run one subcommand."""
    parser = argparse.ArgumentParser(
        prog="sbom.py", description="Build the software bill of materials of a release."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    components = subparsers.add_parser(
        "components", help="write the component list JSON from the build inputs"
    )
    components.add_argument("--metadata", required=True, help="cargo metadata --format-version 1")
    components.add_argument("--lockfile", required=True, help="panel/package-lock.json")
    components.add_argument(
        "--pins", required=True, action="append", help="a pins.txt file (repeatable)"
    )
    components.add_argument("--buildkit", required=True, help="the node agent pins.txt")
    components.add_argument("--out", required=True, help="where to write the component list")
    components.set_defaults(handler=command_components)

    document = subparsers.add_parser(
        "document", help="write the CycloneDX 1.6 document from a component list"
    )
    document.add_argument("--components", required=True, help="a components JSON file")
    document.add_argument("--version", required=True, help="the release version")
    document.add_argument("--tarball", required=True, help="the release tarball")
    document.add_argument(
        "--source-date-epoch", required=True, type=int, help="the release timestamp"
    )
    document.add_argument("--out", required=True, help="where to write the document")
    document.set_defaults(handler=command_document)

    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
