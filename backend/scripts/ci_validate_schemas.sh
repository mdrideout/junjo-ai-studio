#!/bin/bash
# CI/CD script to validate API schemas
#
# This script:
# 1. Exports the OpenAPI schema from the FastAPI app
# 2. Validates critical schemas match frontend expectations
#
# Usage:
#   ./scripts/ci_validate_schemas.sh
#
# Exit codes:
#   0 - Validation passed
#   1 - Validation failed

set -e  # Exit on first error

echo "========================================"
echo "API Schema Validation (CI/CD)"
echo "========================================"
echo ""

# Step 1: Export OpenAPI schema
echo "Step 1: Exporting OpenAPI schema..."
uv run python scripts/export_openapi_schema.py

echo ""
echo "Step 2: Validating frontend schemas match backend..."
uv run python scripts/validate_frontend_schemas.py

echo ""
echo "========================================"
echo "✅ Schema validation completed successfully"
echo "========================================"
