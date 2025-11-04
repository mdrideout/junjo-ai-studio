# API Schema Validation

This directory contains scripts to prevent frontend/backend API contract drift.

## Philosophy

**Backend is the single source of truth.** Frontend Zod schemas must match backend Pydantic schemas.

## How It Works

1. **Export OpenAPI Schema** (`export_openapi_schema.py`)
   - Imports FastAPI app directly (no server needed)
   - Calls `app.openapi()` to generate schema
   - Saves to `openapi.json` (gitignored)

2. **Validate Frontend Schemas** (`validate_frontend_schemas.py`)
   - Parses frontend Zod schemas from TypeScript files
   - Compares against backend OpenAPI schemas
   - Fails if schemas don't match

3. **CI/CD Integration** (`ci_validate_schemas.sh`)
   - Runs both export and validation
   - Exit code 0 = success, 1 = failure

## Usage

### Local Validation

```bash
cd backend
./scripts/ci_validate_schemas.sh
```

### CI/CD (GitHub Actions)

```yaml
- name: Validate API Schemas
  run: |
    cd backend
    uv sync
    ./scripts/ci_validate_schemas.sh
```

## What It Catches

✅ Missing required fields in frontend
✅ Extra fields in frontend (not in backend)
✅ Type mismatches (string vs number, etc.)
✅ PascalCase fields (should be snake_case)

## Adding New Schema Validations

Edit `validate_frontend_schemas.py`:

```python
SCHEMA_MAPPINGS = {
    "frontend/src/features/YOUR_FEATURE/schema.ts": {
        "zod_schema_name": "yourSchema",
        "openapi_schema_name": "YourSchemaRead",
        "description": "Your object description",
    },
}
```

## Example: What Happens When Schemas Drift

If frontend uses PascalCase (wrong):
```typescript
export const userSchema = z.object({
  ID: z.number(),  // ❌ Wrong: should be 'id: z.string()'
  Email: z.string(),  // ❌ Wrong: should be 'email'
})
```

Validation catches it:
```
❌ Field uses PascalCase (should be snake_case): 'ID'
❌ Type mismatch for 'ID': frontend has z.number(), backend is string
```

## No Multiple Sources of Truth

This approach does **not** hardcode expected schemas. It compares:
- Frontend Zod schemas (parsed from TypeScript)
- Backend OpenAPI schemas (generated from Pydantic)

The backend is the source of truth, frontend must match.
