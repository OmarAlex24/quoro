#!/usr/bin/env python3
"""Third-party validation of the machine-readable contracts.

Usage: python3 -I .github/scripts/validate_contracts.py [repo_root]

scripts/check_contracts.py covers what the standard library can check. This
script adds what needs third-party validators (docs/contracts/README.md,
"Validation"):

- docs/contracts/openapi.yaml is a valid OpenAPI 3.1 document;
- docs/contracts/events.schema.json is a valid JSON Schema 2020-12 document
  and every one of its examples validates against it.

Requirements are pinned in .github/scripts/requirements.txt.
"""
import json
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator
from openapi_spec_validator import validate


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[2]
    contracts = root / "docs" / "contracts"

    spec = yaml.safe_load((contracts / "openapi.yaml").read_text(encoding="utf-8"))
    version = str(spec.get("openapi", ""))
    if not version.startswith("3.1."):
        print(f"FAIL openapi.yaml declares openapi {version!r}, expected 3.1.x")
        return 1
    validate(spec)
    print(f"openapi.yaml: valid OpenAPI {version}")

    schema = json.loads((contracts / "events.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    examples = schema.get("examples", [])
    if not examples:
        print("FAIL events.schema.json has no examples")
        return 1
    validator = Draft202012Validator(schema)
    for i, example in enumerate(examples):
        errors = sorted(validator.iter_errors(example), key=lambda e: list(e.path))
        if errors:
            for error in errors:
                print(f"FAIL events.schema.json example {i}: {error.message}")
            return 1
    print(f"events.schema.json: valid JSON Schema 2020-12, {len(examples)} examples valid")
    print("ALL THIRD-PARTY CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
