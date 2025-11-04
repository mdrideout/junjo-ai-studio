#!/usr/bin/env python3
"""
Validate that frontend Zod schemas match backend OpenAPI schemas.

This script compares frontend TypeScript schemas against the backend
OpenAPI specification to ensure they stay in sync. The backend is the
single source of truth.

Usage:
    python scripts/validate_frontend_schemas.py

Exit codes:
    0 - Frontend schemas match backend
    1 - Schema mismatches detected
"""

import json
import re
import sys
from pathlib import Path
from typing import Any

from loguru import logger


# Map of frontend schema files to their corresponding OpenAPI schemas
SCHEMA_MAPPINGS = {
    "frontend/src/features/users/schema.ts": {
        "zod_schema_name": "userSchema",
        "openapi_schema_name": "UserRead",
        "description": "User object",
    },
    "frontend/src/features/api-keys/schemas.ts": {
        "zod_schema_name": "ApiKeySchema",
        "openapi_schema_name": "APIKeyRead",
        "description": "API Key object",
    },
}


def load_openapi_schema(schema_path: Path) -> dict[str, Any]:
    """Load OpenAPI schema from JSON file."""
    logger.info(f"Loading OpenAPI schema from {schema_path}")
    with open(schema_path) as f:
        return json.load(f)


def parse_zod_schema(file_path: Path, schema_name: str) -> dict[str, Any]:
    """Parse a Zod schema from TypeScript file.

    This is a simple parser that extracts field names and basic types.
    It looks for patterns like: field_name: z.string()
    """
    logger.info(f"Parsing Zod schema '{schema_name}' from {file_path}")

    with open(file_path) as f:
        content = f.read()

    # Find the schema definition
    schema_pattern = rf"{schema_name}\s*=\s*z\.object\(\{{([^}}]+)\}}\)"
    match = re.search(schema_pattern, content, re.DOTALL)

    if not match:
        logger.error(f"Could not find schema definition for '{schema_name}'")
        return {}

    schema_body = match.group(1)

    # Parse fields: looking for patterns like "field_name: z.type()"
    field_pattern = r'(\w+):\s*z\.(\w+)\('
    fields = {}

    for field_match in re.finditer(field_pattern, schema_body):
        field_name = field_match.group(1)
        zod_type = field_match.group(2)
        fields[field_name] = zod_type

    return fields


def map_openapi_type_to_zod(openapi_type: str, openapi_format: str | None) -> str:
    """Map OpenAPI type to expected Zod type."""
    if openapi_type == "string":
        if openapi_format == "date-time":
            return "string"  # Zod uses z.string().datetime()
        if openapi_format == "email":
            return "string"  # Zod uses z.string().email()
        return "string"
    elif openapi_type == "boolean":
        return "boolean"
    elif openapi_type == "integer":
        return "number"
    elif openapi_type == "number":
        return "number"
    return openapi_type


def validate_schema_match(
    frontend_file: str,
    zod_schema_name: str,
    openapi_schema_name: str,
    description: str,
    frontend_root: Path,
    openapi_schemas: dict[str, Any],
) -> list[str]:
    """Validate that a frontend Zod schema matches its backend OpenAPI schema."""
    errors = []

    logger.info("")
    logger.info(f"Validating: {description}")
    logger.info(f"  Frontend: {frontend_file}")
    logger.info(f"  Backend:  OpenAPI schema '{openapi_schema_name}'")

    # Check backend schema exists
    if openapi_schema_name not in openapi_schemas:
        error = f"  ❌ Backend schema '{openapi_schema_name}' not found in OpenAPI spec"
        logger.error(error)
        return [error]

    backend_schema = openapi_schemas[openapi_schema_name]
    backend_props = backend_schema.get("properties", {})
    backend_required = backend_schema.get("required", [])

    # Parse frontend schema
    frontend_path = frontend_root / frontend_file
    if not frontend_path.exists():
        error = f"  ❌ Frontend schema file not found: {frontend_path}"
        logger.error(error)
        return [error]

    frontend_fields = parse_zod_schema(frontend_path, zod_schema_name)

    if not frontend_fields:
        error = f"  ❌ Could not parse frontend schema '{zod_schema_name}'"
        logger.error(error)
        return [error]

    # Check for missing fields (in backend but not frontend)
    for field_name in backend_required:
        if field_name not in frontend_fields:
            error = f"  ❌ Missing required field in frontend: '{field_name}'"
            logger.error(error)
            errors.append(error)

    # Check for extra fields (in frontend but not backend)
    for field_name in frontend_fields.keys():
        if field_name not in backend_props:
            error = f"  ❌ Extra field in frontend (not in backend): '{field_name}'"
            logger.error(error)
            errors.append(error)

    # Check field types match
    for field_name, zod_type in frontend_fields.items():
        if field_name not in backend_props:
            continue  # Already reported as extra

        backend_field = backend_props[field_name]
        backend_type = backend_field.get("type")
        backend_format = backend_field.get("format")

        # Handle anyOf (nullable fields)
        if "anyOf" in backend_field:
            # Get the non-null type
            for variant in backend_field["anyOf"]:
                if variant.get("type") != "null":
                    backend_type = variant.get("type")
                    backend_format = variant.get("format")
                    break

        expected_zod_type = map_openapi_type_to_zod(backend_type, backend_format)

        if zod_type != expected_zod_type:
            error = f"  ❌ Type mismatch for '{field_name}': frontend has z.{zod_type}(), backend is {backend_type}"
            logger.error(error)
            errors.append(error)

    # Check for PascalCase fields (common mistake)
    for field_name in frontend_fields.keys():
        if field_name[0].isupper():
            error = f"  ❌ Field uses PascalCase (should be snake_case): '{field_name}'"
            logger.error(error)
            errors.append(error)

    if not errors:
        logger.success("  ✅ Frontend schema matches backend")

    return errors


def main():
    logger.info("=" * 60)
    logger.info("Frontend Schema Validator")
    logger.info("Backend OpenAPI schemas are the source of truth")
    logger.info("=" * 60)

    # Paths
    backend_dir = Path(__file__).parent.parent
    frontend_root = backend_dir.parent
    schema_path = backend_dir / "openapi.json"

    # Load OpenAPI schema
    if not schema_path.exists():
        logger.error(f"OpenAPI schema not found at {schema_path}")
        logger.error("Run 'python scripts/export_openapi_schema.py' first")
        sys.exit(1)

    openapi_schema = load_openapi_schema(schema_path)
    openapi_schemas = openapi_schema.get("components", {}).get("schemas", {})

    # Validate each mapping
    all_errors = []
    for frontend_file, mapping in SCHEMA_MAPPINGS.items():
        errors = validate_schema_match(
            frontend_file=frontend_file,
            zod_schema_name=mapping["zod_schema_name"],
            openapi_schema_name=mapping["openapi_schema_name"],
            description=mapping["description"],
            frontend_root=frontend_root,
            openapi_schemas=openapi_schemas,
        )
        all_errors.extend(errors)

    # Summary
    logger.info("")
    logger.info("=" * 60)
    if all_errors:
        logger.error(f"❌ Validation FAILED with {len(all_errors)} error(s)")
        logger.error("")
        logger.error("Action required:")
        logger.error("  1. Update frontend Zod schemas to match backend")
        logger.error("  2. Backend OpenAPI schemas are the source of truth")
        sys.exit(1)
    else:
        logger.success("✅ All frontend schemas match backend")
        sys.exit(0)


if __name__ == "__main__":
    main()
