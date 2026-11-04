#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Standard-library consistency checks for docs/contracts.

Usage: python3 scripts/check_contracts.py [repo_root]

The default repo root is the parent of this script's directory. Prints one
line per check group, then either "ALL CHECKS PASSED" (exit 0) or one
"FAIL <reason>" line per failure (exit 1).

Nothing here parses YAML: openapi.yaml is checked line by line so the script
stays dependency free. Semantic validation of the OpenAPI document and of the
JSON Schema needs third-party validators and lives in CI, not here.
"""
import json
import os
import re
import sqlite3
import sys

SPECIAL_SCOPES = {"public", "authenticated", "human", "node"}

PATH_KEY_RE = re.compile(r"^  (/[^:]*):\s*$")
OPERATION_RE = re.compile(r"^    (get|post|put|patch|delete):\s*$")
TOP_LEVEL_RE = re.compile(r"^[^\s]")
SCOPE_RE = re.compile(r"^      x-quoro-scope:(.*)$")
MCP_RE = re.compile(r"^      x-quoro-mcp-tool:\s*(\S+)\s*$")
RESPONSE_CODE_RE = re.compile(r"^(\s+)'?([0-9]{3}|default)'?:\s*$")
MD_SCOPE_RE = re.compile(
    r"`((?:cluster|node|project|vm|app|agent|grant|approval|incident|audit|event|webhook|provider|backup|user):[a-z_]+)`"
)


def read(path):
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def section(text, heading):
    """Body of a ``### heading`` section, up to the next ``### `` heading."""
    marker = heading + "\n"
    if marker not in text:
        return None
    return text.split(marker, 1)[1].split("\n### ", 1)[0]


def table_first_cell(body):
    return [m.group(1) for m in re.finditer(r"^\| `([^`]+)`", body, re.M)]


def fail(fails, msg):
    fails.append(msg)


def parse_readme(fails, text):
    scopes_doc = section(text, "### Scopes")
    events_doc = section(text, "### Event types")
    mcp_doc = section(text, "### MCP tools")
    tables_doc = section(text, "### Database tables")
    messages_doc = section(text, "### Node-agent message types")

    for name, doc in (
        ("Scopes", scopes_doc),
        ("Event types", events_doc),
        ("MCP tools", mcp_doc),
        ("Database tables", tables_doc),
        ("Node-agent message types", messages_doc),
    ):
        if doc is None:
            fail(fails, f"README: section {name!r} not found")

    scopes = table_first_cell(scopes_doc or "")
    events = table_first_cell(events_doc or "")
    tools = re.findall(
        r"\| `([a-z_]+)` \| `([A-Z]+ [^`]+)` \| `([^`]+)`", mcp_doc or ""
    )
    tables = re.findall(
        r"`([a-z_]+)`", (tables_doc or "").split("Tables marked", 1)[0]
    )
    messages = re.findall(r"`([a-z_]+)`", messages_doc or "")

    if not scopes:
        fail(fails, "README: no scopes parsed")
    if not events:
        fail(fails, "README: no event types parsed")
    if not tables:
        fail(fails, "README: no database tables parsed")
    if not messages:
        fail(fails, "README: no node-agent message types parsed")
    if not tools:
        fail(fails, "README: no MCP tools parsed")

    print(
        "README: %d scopes, %d event types, %d MCP tools, %d tables, %d node-agent messages"
        % (len(scopes), len(events), len(tools), len(tables), len(messages))
    )
    return scopes, events, tools, tables, messages


def check_events_schema(fails, path, events):
    try:
        schema = json.loads(read(path))
    except json.JSONDecodeError as exc:
        fail(fails, f"events.schema.json: invalid JSON: {exc}")
        return
    except OSError as exc:
        fail(fails, f"events.schema.json: cannot read: {exc}")
        return

    enum = schema.get("properties", {}).get("type", {}).get("enum")
    if not isinstance(enum, list):
        fail(fails, "events.schema.json: properties.type.enum is not a list")
        enum = []
    enum_set = set(enum)
    expected = set(events)
    for missing in sorted(expected - enum_set):
        fail(fails, f"events.schema.json: event type {missing} missing from enum")
    for extra in sorted(enum_set - expected):
        fail(fails, f"events.schema.json: event type {extra} not in README")

    required = schema.get("required", [])
    examples = schema.get("examples", [])
    if not isinstance(examples, list):
        fail(fails, "events.schema.json: examples is not an array")
        examples = []
    if len(examples) < 5:
        fail(fails, f"events.schema.json: only {len(examples)} examples (need >= 5)")

    for index, example in enumerate(examples):
        if not isinstance(example, dict):
            fail(fails, f"events.schema.json: example {index} is not an object")
            continue
        etype = example.get("type")
        if etype not in expected:
            fail(
                fails,
                f"events.schema.json: example {index} type {etype!r} not in README",
            )
        for key in required:
            if key not in example:
                fail(
                    fails,
                    f"events.schema.json: example {index} lacks required key {key!r}",
                )

    print(
        "events.schema.json: %d enum types, %d examples, %d required keys"
        % (len(enum), len(examples), len(required))
    )


MIGRATION_RE = re.compile(r"^([0-9]{4})-[a-z0-9-]+\.sql$")


def check_db_schema(fails, path, tables):
    try:
        script = read(path)
    except OSError as exc:
        fail(fails, f"db-schema.sql: cannot read: {exc}")
        return
    con = sqlite3.connect(":memory:")
    try:
        con.executescript(script)
    except sqlite3.Error as exc:
        fail(fails, f"db-schema.sql: sqlite error: {exc}")
        con.close()
        return

    # Post-freeze schema changes are numbered migrations applied in order on
    # top of the frozen base; each one bumps meta.schema_version by one.
    migrations_dir = os.path.join(os.path.dirname(path), "migrations")
    migrations = []
    if os.path.isdir(migrations_dir):
        for name in sorted(os.listdir(migrations_dir)):
            match = MIGRATION_RE.match(name)
            if not match:
                fail(fails, f"migrations/{name}: name is not NNNN-<slug>.sql")
                continue
            migrations.append((int(match.group(1)), name))
    for expected_number, (number, name) in enumerate(migrations, start=1):
        if number != expected_number:
            fail(fails, f"migrations/{name}: expected number {expected_number:04d}")
            con.close()
            return
        try:
            con.executescript(read(os.path.join(migrations_dir, name)))
        except (OSError, sqlite3.Error) as exc:
            fail(fails, f"migrations/{name}: {exc}")
            con.close()
            return
    version = con.execute(
        "select value from meta where key = 'schema_version'"
    ).fetchone()
    expected_version = str(1 + len(migrations))
    if version is None or version[0] != expected_version:
        fail(
            fails,
            f"db schema: schema_version {version and version[0]!r}, "
            f"expected {expected_version!r} after {len(migrations)} migrations",
        )
    names = {
        row[0]
        for row in con.execute("select name from sqlite_master where type='table'")
    }
    con.close()
    names.discard("sqlite_sequence")

    expected = set(tables)
    for missing in sorted(expected - names):
        fail(fails, f"db-schema.sql: table {missing} missing from schema")
    for extra in sorted(names - expected):
        fail(fails, f"db-schema.sql: table {extra} not in README")

    print(
        "db-schema.sql: %d tables, loaded into sqlite with %d migrations"
        % (len(names), len(migrations))
    )


def operation_blocks(lines):
    """Yield (method, path, start, end) for each path operation."""
    ops = []
    current_path = None
    for index, line in enumerate(lines):
        match = PATH_KEY_RE.match(line)
        if match:
            current_path = match.group(1)
            continue
        if TOP_LEVEL_RE.match(line):
            current_path = None
            continue
        match = OPERATION_RE.match(line)
        if match and current_path is not None:
            ops.append((match.group(1).upper(), current_path, index))
    blocks = []
    for method, path, start in ops:
        end = len(lines)
        for index in range(start + 1, len(lines)):
            line = lines[index]
            if (
                PATH_KEY_RE.match(line)
                or OPERATION_RE.match(line)
                or TOP_LEVEL_RE.match(line)
            ):
                end = index
                break
        blocks.append((method, path, start, end))
    return blocks


def parse_scope(block_lines):
    """Return (raw_value, values) for an operation block, or (None, [])."""
    for offset, line in enumerate(block_lines):
        match = SCOPE_RE.match(line)
        if not match:
            continue
        rest = match.group(1).strip()
        if rest == "":
            values = []
            for follow in block_lines[offset + 1 :]:
                item = re.match(r"^\s+-\s+(.+?)\s*$", follow)
                if not item:
                    break
                values.append(item.group(1).strip("'\""))
            return "", values
        if rest.startswith("[") and rest.endswith("]"):
            inner = rest[1:-1]
            values = [part.strip().strip("'\"") for part in inner.split(",")]
            values = [value for value in values if value]
            return rest, values
        return rest, [rest.strip("'\"")]
    return None, []


def check_openapi(fails, path, scopes, events, tools):
    try:
        text = read(path)
    except OSError as exc:
        fail(fails, f"openapi.yaml: cannot read: {exc}")
        return
    lines = text.splitlines()

    if not any(re.match(r"^openapi:\s*3\.1\.\d+\s*$", line) for line in lines):
        fail(fails, "openapi.yaml: no 'openapi: 3.1.x' line")

    scope_set = set(scopes)
    blocks = operation_blocks(lines)
    annotated = {}
    path_count = len({path for _, path, _, _ in blocks})

    for method, op_path, start, end in blocks:
        block = lines[start + 1 : end]
        raw_scope, values = parse_scope(block)

        if raw_scope is None:
            fail(fails, f"openapi.yaml: {method} {op_path} has no x-quoro-scope")
        else:
            for value in values:
                if value not in scope_set and value not in SPECIAL_SCOPES:
                    fail(
                        fails,
                        f"openapi.yaml: {method} {op_path} unknown scope {value}",
                    )
            if "public" in values and not any(
                line.strip() == "security: []" for line in block
            ):
                fail(
                    fails,
                    f"openapi.yaml: {method} {op_path} is public but has no 'security: []'",
                )

        tool_name = None
        for line in block:
            match = MCP_RE.match(line)
            if match:
                tool_name = match.group(1)
                break
        if tool_name:
            scope_text = raw_scope if raw_scope else ", ".join(values)
            annotated[tool_name] = (method, op_path, scope_text)

        # responses inside the operation
        responses_at = None
        for offset, line in enumerate(block):
            if line.strip() == "responses:":
                responses_at = offset
                break
        if responses_at is not None:
            response_lines = block[responses_at + 1 :]
            code_indent = None
            code_starts = []
            for offset, line in enumerate(response_lines):
                match = RESPONSE_CODE_RE.match(line)
                if not match:
                    continue
                if code_indent is None:
                    code_indent = len(match.group(1))
                if len(match.group(1)) == code_indent:
                    code_starts.append((offset, match.group(2)))
            for position, (offset, code) in enumerate(code_starts):
                stop = (
                    code_starts[position + 1][0]
                    if position + 1 < len(code_starts)
                    else len(response_lines)
                )
                body = response_lines[offset + 1 : stop]
                if not any(
                    "description:" in line or "$ref:" in line for line in body
                ):
                    fail(
                        fails,
                        f"openapi.yaml: {method} {op_path} response {code} "
                        "has no description or $ref",
                    )

    # components.responses: names at indentation 4
    components_at = None
    for index, line in enumerate(lines):
        if line.rstrip() == "components:":
            components_at = index
            break
    if components_at is None:
        fail(fails, "openapi.yaml: no components section")
    else:
        responses_at = None
        for index in range(components_at + 1, len(lines)):
            line = lines[index]
            if TOP_LEVEL_RE.match(line):
                break
            if line.rstrip() == "  responses:":
                responses_at = index
                break
        if responses_at is None:
            fail(fails, "openapi.yaml: no components.responses section")
        else:
            end = len(lines)
            for index in range(responses_at + 1, len(lines)):
                line = lines[index]
                if TOP_LEVEL_RE.match(line) or re.match(r"^  \S", line):
                    end = index
                    break
            section_lines = lines[responses_at + 1 : end]
            names = []
            for offset, line in enumerate(section_lines):
                match = re.match(r"^    ([A-Za-z_][A-Za-z0-9_]*):\s*$", line)
                if match:
                    names.append((offset, match.group(1)))
            for position, (offset, name) in enumerate(names):
                stop = (
                    names[position + 1][0]
                    if position + 1 < len(names)
                    else len(section_lines)
                )
                body = section_lines[offset + 1 : stop]
                if not any("description:" in line for line in body):
                    fail(
                        fails,
                        f"openapi.yaml: components.responses.{name} has no description",
                    )
            print(
                "openapi.yaml: %d paths, %d operations, %d component responses"
                % (path_count, len(blocks), len(names))
            )

    # MCP tools: README row vs annotation
    for tool, endpoint, scope in tools:
        method, tool_path = endpoint.split(" ", 1)
        if tool not in annotated:
            fail(
                fails,
                f"openapi.yaml: MCP tool {tool} is not annotated on any operation",
            )
            continue
        got_method, got_path, got_scope = annotated[tool]
        if (got_method, got_path) != (method, tool_path):
            fail(
                fails,
                f"openapi.yaml: MCP tool {tool} annotated on {got_method} {got_path} "
                f"but README says {endpoint}",
            )
        elif got_scope != scope:
            fail(
                fails,
                f"openapi.yaml: MCP tool {tool} scope {got_scope!r} "
                f"but README says {scope!r}",
            )

    tool_names = {tool for tool, _, _ in tools}
    for tool in sorted(set(annotated) - tool_names):
        fail(fails, f"openapi.yaml: MCP tool {tool} annotated but not in README")

    for event in events:
        if event not in text:
            fail(fails, f"openapi.yaml: event type {event} is never mentioned")


def check_markdown(fails, directory, scopes):
    names = [
        "permission-model.md",
        "audit-log-format.md",
        "node-agent-protocol.md",
        "engine-trait.md",
        "backup-format.md",
    ]
    scope_set = set(scopes)
    total = 0
    for name in names:
        file_path = os.path.join(directory, name)
        try:
            text = read(file_path)
        except OSError as exc:
            fail(fails, f"{name}: cannot read: {exc}")
            continue
        found = set(MD_SCOPE_RE.findall(text))
        total += len(found)
        for scope in sorted(found - scope_set):
            fail(fails, f"{name}: unknown scope {scope}")
    print("markdown contracts: %d files, %d distinct scope-like tokens" % (len(names), total))


def check_node_agent(fails, directory, messages):
    file_path = os.path.join(directory, "node-agent-protocol.md")
    try:
        text = read(file_path)
    except OSError as exc:
        fail(fails, f"node-agent-protocol.md: cannot read: {exc}")
        return
    lines = text.splitlines()

    headings = re.findall(r"^### 5\.\d+ `([a-z_]+)`", text, re.M)
    expected = set(messages)
    got = set(headings)
    for missing in sorted(expected - got):
        fail(
            fails,
            f"node-agent-protocol.md: message {missing} is in README but has no heading",
        )
    for extra in sorted(got - expected):
        fail(
            fails,
            f"node-agent-protocol.md: message {extra} has a heading but is not in README",
        )

    blocks = 0
    for index, line in enumerate(lines):
        if line.strip() != "```json":
            continue
        close = None
        for cursor in range(index + 1, len(lines)):
            if lines[cursor].strip() == "```":
                close = cursor
                break
        if close is None:
            fail(
                fails,
                f"node-agent-protocol.md:{index + 1}: JSON block is never closed",
            )
            continue
        body = "\n".join(lines[index + 1 : close])
        blocks += 1
        try:
            json.loads(body)
        except json.JSONDecodeError as exc:
            fail(
                fails,
                f"node-agent-protocol.md:{index + 1}: JSON block does not parse: {exc}",
            )

    print(
        "node-agent-protocol.md: %d message headings, %d JSON blocks"
        % (len(headings), blocks)
    )


def main(argv):
    if len(argv) > 1:
        root = os.path.abspath(argv[1])
    else:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    contracts = os.path.join(root, "docs", "contracts")

    fails = []
    readme_path = os.path.join(contracts, "README.md")
    try:
        readme_text = read(readme_path)
    except OSError as exc:
        print(f"FAIL README.md: cannot read: {exc}")
        return 1

    scopes, events, tools, tables, messages = parse_readme(fails, readme_text)
    check_events_schema(fails, os.path.join(contracts, "events.schema.json"), events)
    check_db_schema(fails, os.path.join(contracts, "db-schema.sql"), tables)
    check_openapi(
        fails, os.path.join(contracts, "openapi.yaml"), scopes, events, tools
    )
    check_markdown(fails, contracts, scopes)
    check_node_agent(fails, contracts, messages)

    if fails:
        for message in fails:
            print(f"FAIL {message}")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
