#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Standard-library checks for the mdBook documentation site.

Usage: python3 -I scripts/check_docs_site.py [repo_root]

Run it after `mdbook build docs`. mdBook has no built-in link checker, so
this script does two things:

1. Every Markdown file under docs/ is listed in docs/SUMMARY.md. mdBook only
   renders listed chapters, so an unlisted file (a new ADR, for example)
   would silently be missing from the site.
2. Every relative link in the built HTML (target/book) resolves: the target
   file exists, and a #fragment names an element id on the target page.
   External links (http, https, mailto) are not fetched.

Prints one "FAIL <reason>" line per problem and exits 1, or prints
"ALL CHECKS PASSED" and exits 0.
"""
import os
import re
import sys
from html.parser import HTMLParser
from urllib.parse import unquote, urlsplit

SUMMARY_LINK_RE = re.compile(r"\]\(([^)\s]+\.md)\)")
# Pages mdBook generates itself; they are not chapters.
GENERATED_PAGES = {"404.html", "print.html", "toc.html"}


class PageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.ids = set()
        self.links = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        for key in ("id", "name"):
            if attrs.get(key):
                self.ids.add(attrs[key])
        for key in ("href", "src"):
            if attrs.get(key) is not None:
                self.links.append((tag, key, attrs[key]))


def check_summary(docs_dir, fails):
    with open(os.path.join(docs_dir, "SUMMARY.md"), encoding="utf-8") as fh:
        listed = {os.path.normpath(p) for p in SUMMARY_LINK_RE.findall(fh.read())}
    for root, _dirs, files in os.walk(docs_dir):
        for name in files:
            if not name.endswith(".md"):
                continue
            rel = os.path.relpath(os.path.join(root, name), docs_dir)
            if rel == "SUMMARY.md":
                continue
            if rel not in listed:
                fails.append(f"docs/{rel} is not listed in docs/SUMMARY.md")
    for rel in sorted(listed):
        if not os.path.isfile(os.path.join(docs_dir, rel)):
            fails.append(f"docs/SUMMARY.md lists docs/{rel}, which does not exist")
    return len(listed)


def check_links(book_dir, fails):
    pages = {}
    for root, _dirs, files in os.walk(book_dir):
        for name in files:
            if name.endswith(".html"):
                path = os.path.join(root, name)
                parser = PageParser()
                with open(path, encoding="utf-8") as fh:
                    parser.feed(fh.read())
                pages[os.path.normpath(path)] = parser

    checked = 0
    for page, parser in sorted(pages.items()):
        rel_page = os.path.relpath(page, book_dir)
        for tag, attr, value in parser.links:
            url = urlsplit(value)
            if url.scheme or url.netloc or value.startswith("javascript:"):
                continue
            # mdBook writes a site-root template link into 404.html only.
            if rel_page in GENERATED_PAGES and value.startswith("/"):
                continue
            checked += 1
            if url.path:
                target = os.path.normpath(
                    os.path.join(os.path.dirname(page), unquote(url.path))
                )
                if os.path.isdir(target):
                    target = os.path.join(target, "index.html")
            else:
                target = page
            where = f"{rel_page}: <{tag} {attr}={value!r}>"
            if not os.path.isfile(target):
                fails.append(f"{where} points at a missing file")
                continue
            if url.fragment and target.endswith(".html"):
                ids = pages[os.path.normpath(target)].ids
                if unquote(url.fragment) not in ids:
                    fails.append(f"{where} names a missing anchor")
    return len(pages), checked


def main():
    repo = os.path.abspath(
        sys.argv[1]
        if len(sys.argv) > 1
        else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
    )
    docs_dir = os.path.join(repo, "docs")
    book_dir = os.path.join(repo, "target", "book")
    fails = []

    chapters = check_summary(docs_dir, fails)
    print(f"summary: {chapters} chapters listed")
    if not os.path.isfile(os.path.join(book_dir, "index.html")):
        fails.append(f"{book_dir}/index.html not found; run `mdbook build docs` first")
    else:
        pages, links = check_links(book_dir, fails)
        print(f"links: {links} relative links checked in {pages} pages")

    for msg in fails:
        print(f"FAIL {msg}")
    if fails:
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
