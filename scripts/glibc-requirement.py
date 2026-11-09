#!/usr/bin/env python3
"""Print each ELF's maximum required GLIBC version and the overall maximum as JSON.

Read .gnu.version_r directly, without binutils. Optionally reject requirements
above --floor VERSION; versions are compared as numeric components.
"""
import argparse
import json
import re
import struct
from pathlib import Path


def version(value):
    if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", value):
        raise ValueError(f"invalid GLIBC version: {value}")
    parts = tuple(map(int, value.split(".")))
    while len(parts) > 1 and parts[-1] == 0:
        parts = parts[:-1]
    return parts


def requirement(path):
    with Path(path).open("rb") as stream:
        size = stream.seek(0, 2)

        def read(offset, length):
            if offset < 0 or length < 0 or offset + length > size:
                raise ValueError(f"{path}: truncated ELF")
            stream.seek(offset)
            data = stream.read(length)
            if len(data) != length:
                raise ValueError(f"{path}: truncated ELF")
            return data

        ident = read(0, 16)
        if ident[:4] != b"\x7fELF" or ident[4] not in (1, 2) or ident[5] not in (1, 2):
            raise ValueError(f"{path}: unsupported ELF header")
        endian = "<" if ident[5] == 1 else ">"
        header_format = endian + ("HHIIIIIHHHHHH" if ident[4] == 1 else "HHIQQQIHHHHHH")
        header = struct.unpack(header_format, read(16, struct.calcsize(header_format)))
        section_offset, entry_size, count, names_index = header[5], header[10], header[11], header[12]
        section_format = endian + ("IIIIIIIIII" if ident[4] == 1 else "IIQQQQIIQQ")
        section_size = struct.calcsize(section_format)
        if not section_offset or entry_size < section_size:
            raise ValueError(f"{path}: ELF section table is missing or invalid")

        def section(index):
            return struct.unpack(section_format, read(section_offset + index * entry_size, section_size))

        first = section(0)
        count = count or first[5]
        names_index = first[6] if names_index == 0xffff else names_index
        if not count or names_index >= count or section_offset + count * entry_size > size:
            raise ValueError(f"{path}: invalid ELF section table")
        names_section = section(names_index)
        names = read(names_section[4], names_section[5])

        def string(table, offset):
            end = table.find(b"\0", offset)
            if offset < 0 or end < offset:
                raise ValueError(f"{path}: invalid ELF string")
            return table[offset:end].decode("ascii")

        required = []
        for index in range(count):
            entry = section(index)
            if string(names, entry[0]) != ".gnu.version_r":
                continue
            if entry[1] != 0x6ffffffe or entry[6] >= count:
                raise ValueError(f"{path}: invalid .gnu.version_r")
            data = read(entry[4], entry[5])
            strings_section = section(entry[6])
            strings = read(strings_section[4], strings_section[5])

            def unpack(fmt, offset):
                length = struct.calcsize(endian + fmt)
                if offset < 0 or offset + length > len(data):
                    raise ValueError(f"{path}: truncated .gnu.version_r")
                return struct.unpack_from(endian + fmt, data, offset)

            offset = 0
            while data:
                revision, auxiliary_count, _, auxiliary_offset, next_offset = unpack("HHIII", offset)
                if revision != 1:
                    raise ValueError(f"{path}: unsupported version requirement revision")
                auxiliary = offset + auxiliary_offset
                for number in range(auxiliary_count):
                    _, _, _, name_offset, next_auxiliary = unpack("IHHII", auxiliary)
                    name = string(strings, name_offset)
                    match = re.fullmatch(r"GLIBC_([0-9]+(?:\.[0-9]+)+)", name)
                    if match:
                        required.append(match[1])
                    if number + 1 < auxiliary_count and next_auxiliary < 16:
                        raise ValueError(f"{path}: invalid version auxiliary chain")
                    auxiliary += next_auxiliary
                if not next_offset:
                    break
                if next_offset < 16:
                    raise ValueError(f"{path}: invalid version requirement chain")
                offset += next_offset
        return max(required, key=version) if required else None


def check_floor(maximum, floor):
    if maximum is not None and version(maximum) > version(floor):
        raise ValueError(f"required GLIBC {maximum} exceeds GLIBC_FLOOR {floor}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--floor", help="maximum permitted GLIBC requirement")
    parser.add_argument("elf", nargs="+")
    args = parser.parse_args()
    try:
        result = {path: requirement(path) for path in args.elf}
        result["maximum"] = max((v for v in result.values() if v is not None), key=version, default=None)
        if args.floor is not None:
            version(args.floor)
            check_floor(result["maximum"], args.floor)
    except (OSError, ValueError, struct.error) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
