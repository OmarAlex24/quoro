#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Regenerates the rpmdb SQLite fixtures read by the sqlite/rpm tests.

Run with `python3 -I testdata/generate.py` from the repository root. The
output is deterministic, so a regenerated file is byte-identical and the
fixtures stay small enough (< 200 KiB) to commit.
"""

import os
import sqlite3
import struct

HERE = os.path.dirname(os.path.abspath(__file__))

TAG_NAME = 1000
TAG_VERSION = 1001
TAG_RELEASE = 1002
TAG_EPOCH = 1003
TAG_ARCH = 1022
TAG_SOURCERPM = 1044
TAG_DESCRIPTION = 1005

TYPE_INT32 = 4
TYPE_STRING = 6


def header_blob(entries, data):
    out = struct.pack(">II", len(entries), len(data))
    for tag, kind, offset, count in entries:
        out += struct.pack(">IIII", tag, kind, offset, count)
    return out + data


def package_blob(name, version, release, arch, sourcerpm, epoch=None, extra=b""):
    data = bytearray()
    entries = []

    def add_string(tag, text):
        offset = len(data)
        data.extend(text.encode() if isinstance(text, str) else text)
        data.append(0)
        entries.append((tag, TYPE_STRING, offset, 1))

    add_string(TAG_NAME, name)
    add_string(TAG_VERSION, version)
    add_string(TAG_RELEASE, release)
    if epoch is not None:
        offset = len(data)
        data.extend(struct.pack(">i", epoch))
        entries.append((TAG_EPOCH, TYPE_INT32, offset, 1))
    add_string(TAG_ARCH, arch)
    add_string(TAG_SOURCERPM, sourcerpm)
    if extra:
        # Unknown to the reader: it only makes the payload spill to overflow
        # pages.
        add_string(TAG_DESCRIPTION, extra)
    return header_blob(entries, bytes(data))


def build(path, page_size, rows):
    if os.path.exists(path):
        os.remove(path)
    connection = sqlite3.connect(path)
    connection.execute(f"PRAGMA page_size={page_size}")
    connection.execute("PRAGMA journal_mode=DELETE")
    connection.execute("CREATE TABLE Packages (hnum INTEGER PRIMARY KEY, blob BLOB)")
    connection.executemany(
        "INSERT INTO Packages (hnum, blob) VALUES (NULL, ?)", [(blob,) for blob in rows]
    )
    connection.commit()
    page_count = connection.execute("PRAGMA page_count").fetchone()[0]
    connection.close()
    return page_count


def main():
    rows = []
    for index in range(300):
        name = f"pkg{index:04d}"
        extra = b"x" * 5000 if index == 5 else b""
        rows.append(
            package_blob(
                name,
                "1.0",
                "1.el9",
                "x86_64",
                f"{name}-1.0-1.el9.src.rpm",
                extra=extra,
            )
        )
    rows.append(
        package_blob(
            "epochy",
            "2.0",
            "3",
            "x86_64",
            "epochy-2.0-3.el9.src.rpm",
            epoch=1,
        )
    )
    path = os.path.join(HERE, "rpmdb-1024.sqlite")
    pages = build(path, 1024, rows)
    print(f"{path}: {len(rows)} rows, {pages} pages, {os.path.getsize(path)} bytes")

    rows = []
    for index in range(58):
        name = f"pkg{index:04d}"
        extra = b"y" * 5000 if index == 3 else b""
        rows.append(
            package_blob(
                name,
                "6.8.0",
                f"{index}.el10",
                "x86_64",
                f"{name}-6.8.0-{index}.el10.src.rpm",
                extra=extra,
            )
        )
    rows.append(
        package_blob(
            "gpg-pubkey",
            "d4082792",
            "5b32db75",
            "noarch",
            "gpg-pubkey-d4082792-5b32db75.src.rpm",
        )
    )
    rows.append(
        package_blob(
            "kernel",
            "6.8.0",
            "1.el10",
            "x86_64",
            "kernel-6.8.0-1.el10.src.rpm",
        )
    )
    path = os.path.join(HERE, "rpmdb-4096.sqlite")
    pages = build(path, 4096, rows)
    print(f"{path}: {len(rows)} rows, {pages} pages, {os.path.getsize(path)} bytes")


if __name__ == "__main__":
    main()
