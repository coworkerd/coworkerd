"""Generate the non-experimental flag allowlist from the pinned workerd schema."""
import hashlib
import json
from pathlib import Path
import re
import sys

root = Path(__file__).resolve().parents[1]
source = root / "workerd/src/workerd/io/compatibility-date.capnp"
raw = source.read_bytes()
schema = re.sub(r"#[^\n]*", "", raw.decode())
flags = set()
for field in re.findall(r"\w+\s+@\d+\s*:\s*Bool\b[^;]*;", schema):
    if "$experimental" not in field:
        flags.update(re.findall(r'\$compat(?:Enable|Disable)Flag\("([^"\n]+)"\)', field))
flags = sorted(flags)
assert flags and "nodejs_compat" in flags and "enable_request_signal" in flags
upstream = dict(line.split("=", 1) for line in (root / "patches/workerd/UPSTREAM").read_text().splitlines() if line)
version = upstream["commit"]
value = {"workerdVersion": upstream["tag"], "workerdCommit": version,
    "sourceSha256": hashlib.sha256(raw).hexdigest(), "maximumCompatibilityDate":
    (root / "workerd/src/workerd/io/maximum-compatibility-date.txt").read_text().strip(), "flags": flags}
output = Path(sys.argv[1]) if len(sys.argv) > 1 else root / "common/generated"
output.mkdir(parents=True, exist_ok=True)
(output / "compatibility-flags.json").write_text(json.dumps(value, indent=2) + "\n")
(output / "compatibility-flags.h").write_text('#pragma once\n#include <array>\nnamespace coworkerd {\n'
    'inline constexpr const char* MAX_COMPATIBILITY_DATE = "' + value['maximumCompatibilityDate'] + '";\n'
    'inline constexpr std::array<const char*, ' + str(len(flags)) + '> COMPATIBILITY_FLAGS = {\n'
    + ''.join('  "' + flag + '",\n' for flag in flags) + '};\n}\n')
print(json.dumps({"flags": len(flags), "sourceSha256": value['sourceSha256']}))
