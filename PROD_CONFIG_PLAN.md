# Junjo AI Studio Production Configuration Refactor Plan

**Status**: Planned
**Date**: 2025-01-15
**Issue**: `JUNJO_PROD_AUTH_DOMAIN` environment variable removal and production URL configuration strategy

---

## Executive Summary

### Problem Statement

The current production configuration uses an **undocumented environment variable** (`JUNJO_PROD_AUTH_DOMAIN`) with **hardcoded assumptions** about URL structure:

1. **Undocumented**: `JUNJO_PROD_AUTH_DOMAIN` is used in `frontend/prod-startup.sh` but not in `.env.example`
2. **Inflexible**: Assumes `api.` subdomain prefix for backend (line 32: `API_HOST="https://api.${JUNJO_PROD_AUTH_DOMAIN}"`)
3. **Limited Support**: Doesn't support custom subdomain patterns (e.g., `backend.example.com`, `service.example.com`)
4. **No OTLP Guidance**: Client SDKs have no configuration reference for OTLP ingestion endpoint

### Recommended Solution

**Hybrid Explicit Configuration (Option C)**: Explicit URLs where it matters, smart defaults where predictable.

**Minimum Required Configuration (2 variables):**
```bash
JUNJO_PROD_FRONTEND_URL=https://app.example.com
JUNJO_PROD_BACKEND_URL=https://api.example.com
```

**Auto-Derived (with override capability):**
- `JUNJO_PROD_OTLP_ENDPOINT` → defaults to `${BACKEND_URL}:50051`
- `JUNJO_ALLOW_ORIGINS` → defaults to `${FRONTEND_URL}`

**Built-in Validation:**
- Require URLs when `JUNJO_ENV=production`
- Validate same-domain requirement (session cookie compatibility)
- Display computed configuration at startup

---

## Current State Analysis

### Environment Variables Inventory

#### Currently Defined in `.env.example`:

**Build & Runtime:**
- `JUNJO_BUILD_TARGET` - Docker build stage (development/production)
- `JUNJO_ENV` - Runtime environment (development/production)

**Backend:**
- `JUNJO_SESSION_SECRET` - Session signing key
- `JUNJO_SECURE_COOKIE_KEY` - Cookie encryption key
- `JUNJO_ALLOW_ORIGINS` - CORS allowed origins
- `JUNJO_BACKEND_PORT` - Backend HTTP port (1323)
- `JUNJO_LOG_LEVEL` - Logging level
- `JUNJO_LOG_FORMAT` - Log format

**Database:**
- `JUNJO_HOST_DB_DATA_PATH` - Host mount path
- (Internal: `JUNJO_SQLITE_PATH`, `JUNJO_DUCKDB_PATH`, `JUNJO_BADGERDB_PATH`)

**Service Communication (Docker-only):**
- `INGESTION_HOST`, `INGESTION_PORT` - Backend → Ingestion
- `BACKEND_GRPC_HOST`, `BACKEND_GRPC_PORT` - Ingestion → Backend

**AI Providers:**
- `GEMINI_API_KEY`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`

#### Used but NOT Defined:

- **`JUNJO_PROD_AUTH_DOMAIN`** ❌ - Used in `frontend/prod-startup.sh` but undocumented

### Service Communication Matrix

| From Service | To Service | How It Finds It | Port | Purpose |
|--------------|-----------|-----------------|------|---------|
| **Frontend** | Backend | `window.runtimeConfig.API_HOST` | 1323 | HTTP API calls |
| **Backend** | Ingestion | `INGESTION_HOST:INGESTION_PORT` | 50052 | Internal gRPC (span reading) |
| **Ingestion** | Backend | `BACKEND_GRPC_HOST:BACKEND_GRPC_PORT` | 50053 | Internal gRPC (API key validation) |
| **Client SDKs** | Ingestion | User-configured OTLP endpoint | 50051 | Public OTLP ingestion |

### Configuration Gaps

#### Critical Gaps:

1. **Undocumented Variable**: `JUNJO_PROD_AUTH_DOMAIN` not in `.env.example`
2. **Hardcoded Convention**: Assumes `api.${DOMAIN}` pattern
3. **No OTLP Config**: Client SDKs need manual configuration, no guidance
4. **No Flexibility**: Doesn't support custom subdomains or apex deployments

#### Minor Gaps:

5. **Manual CORS**: Users must set `JUNJO_ALLOW_ORIGINS` (could auto-derive)
6. **No Port Override**: Assumes 50051 for gRPC (some platforms differ)

---

## Deployment Patterns Analysis

### Common Patterns:

#### Pattern 1: Subdomain + Subdomain (Most Common)
```
Frontend:  https://app.example.com
Backend:   https://api.example.com
Ingestion: https://api.example.com:50051
```
✅ Works with new config
✅ Cookie domain: Automatic (same eTLD+1)

#### Pattern 2: Apex + Subdomain
```
Frontend:  https://example.com
Backend:   https://api.example.com
Ingestion: https://api.example.com:50051
```
✅ Works with new config
✅ Cookie domain: Automatic

#### Pattern 3: Subdomain + Apex
```
Frontend:  https://app.example.com
Backend:   https://example.com
Ingestion: https://example.com:50051
```
✅ Works with new config
✅ Cookie domain: Automatic

#### Pattern 4: Custom Subdomains
```
Frontend:  https://studio.example.com
Backend:   https://backend.example.com
Ingestion: https://backend.example.com:50051
```
❌ **BROKEN with current implementation** (assumes `api.` prefix)
✅ Works with new config

#### Pattern 5: Platform-Specific (Cloud Run, Railway)
```
Frontend:  https://custom-domain.com
Backend:   https://api.custom-domain.com
Ingestion: https://api.custom-domain.com:443  ← Note: port 443, not 50051
```
❌ **BROKEN with current implementation** (wrong port)
✅ Works with new config (override via `JUNJO_PROD_OTLP_ENDPOINT`)

---

## Configuration Strategy Evaluation

### Option A: Single Domain Variable + Convention ❌

```bash
JUNJO_PROD_DOMAIN=example.com
JUNJO_PROD_FRONTEND_SUBDOMAIN=app
JUNJO_PROD_BACKEND_SUBDOMAIN=api
```

**Pros**: Fewer variables (1-3)
**Cons**: Less flexible, port assumptions, complex apex vs subdomain logic

### Option B: Explicit URLs for Everything ⚠️

```bash
JUNJO_PROD_FRONTEND_URL=https://app.example.com
JUNJO_PROD_BACKEND_URL=https://api.example.com
JUNJO_PROD_OTLP_ENDPOINT=https://api.example.com:50051
JUNJO_ALLOW_ORIGINS=https://app.example.com
```

**Pros**: Maximum flexibility, no assumptions
**Cons**: Too many variables (4+), redundancy, misconfiguration risk

### Option C: Hybrid Approach ✅ **RECOMMENDED**

```bash
# Required
JUNJO_PROD_FRONTEND_URL=https://app.example.com
JUNJO_PROD_BACKEND_URL=https://api.example.com

# Optional (auto-derived)
JUNJO_PROD_OTLP_ENDPOINT=  # Defaults to ${BACKEND_URL}:50051
```

**Auto-derivation:**
- OTLP endpoint from backend URL + `:50051`
- CORS origins from frontend URL
- Validation: ensure domains match (same eTLD+1)

**Pros**: Balance of explicitness and convenience, works for 95% of deployments, supports edge cases
**Cons**: Slightly more complex implementation, need URL parsing

**Why Option C?**
1. **User Experience**: 2 variables for standard deployments, 1 override for edge cases
2. **Maintainability**: Auto-derivation reduces errors
3. **Flexibility**: Still supports all deployment patterns
4. **Addresses User Feedback**: "It does not seem practical to not explicitly set this value" → Agreed, explicit is better!

---

## Implementation Plan

### Phase 1: Configuration Files

#### 1.1. Update `.env.example`

Add new production configuration section:

```bash
# === PRODUCTION DEPLOYMENT CONFIGURATION =============================================
# REQUIRED: Only needed when JUNJO_ENV=production
#
# IMPORTANT: Frontend and Backend MUST be on same domain for session cookies to work
# Valid: app.example.com + api.example.com ✅
# Invalid: app.example.com + service.run.app ❌
# See: docs/DEPLOYMENT.md for details

# Frontend URL (where users access the web UI)
# Must include protocol (https://) and domain
# Example: https://app.example.com
# JUNJO_PROD_FRONTEND_URL=

# Backend API URL (where frontend sends HTTP requests)
# Must include protocol (https://) and domain
# Must share same root domain as frontend (for session cookies)
# Example: https://api.example.com
# JUNJO_PROD_BACKEND_URL=

# OTLP Ingestion Endpoint (where OpenTelemetry SDKs send traces)
# OPTIONAL: Auto-derived from backend URL if not set
# Format: protocol://host:port (usually gRPC on port 50051)
# Override if using non-standard port (e.g., Cloud Run uses 443)
# Example: https://api.example.com:50051
# JUNJO_PROD_OTLP_ENDPOINT=

# ====================================================================================
# REMOVED VARIABLE (no longer needed):
# JUNJO_PROD_AUTH_DOMAIN - Replaced by explicit URL configuration above
# ====================================================================================
```

Update existing `JUNJO_ALLOW_ORIGINS` documentation:

```bash
# Allowed Origins:
# A comma-separated list of allowed origins for CORS.
# OPTIONAL in production: Auto-derived from JUNJO_PROD_FRONTEND_URL if not set
# REQUIRED in development: Set to your local frontend URL
# Example: JUNJO_ALLOW_ORIGINS=http://localhost:5151,http://localhost:5153
JUNJO_ALLOW_ORIGINS=http://localhost:5151,http://localhost:5153
```

#### 1.2. Refactor `frontend/prod-startup.sh`

**Current implementation (lines 19-34):**
```bash
# Default FRONTEND_HOST to access the frontend.
FRONTEND_HOST="http://localhost:5153"

# Default API_HOST to localhost for development environments.
API_HOST="http://localhost:1323"

# If running in production, construct the API host from the auth domain.
if [ "$JUNJO_ENV" = "production" ]; then

  # Set the API_HOST and FRONTEND_HOST
  if [ -n "$JUNJO_PROD_AUTH_DOMAIN" ]; then
    FRONTEND_HOST="https://${JUNJO_PROD_AUTH_DOMAIN}"
    API_HOST="https://api.${JUNJO_PROD_AUTH_DOMAIN}"  # ❌ HARDCODED
  fi
fi
```

**New implementation:**
```bash
#!/bin/sh

# Junjo AI Studio Frontend Production Startup Script
#
# Generates runtime configuration for the frontend production build.
# The production frontend is a static build served by nginx, so runtime
# configuration is injected via a JavaScript file that sets window.runtimeConfig.

# === Runtime Configuration ==========================================================
# API_HOST: Backend API URL
# - Development default: http://localhost:1323
# - Production: Use JUNJO_PROD_BACKEND_URL environment variable

if [ "$JUNJO_ENV" = "production" ]; then
  # Validate required production variables
  if [ -z "$JUNJO_PROD_FRONTEND_URL" ]; then
    echo "ERROR: JUNJO_PROD_FRONTEND_URL is required when JUNJO_ENV=production"
    echo "See .env.example for configuration details"
    exit 1
  fi

  if [ -z "$JUNJO_PROD_BACKEND_URL" ]; then
    echo "ERROR: JUNJO_PROD_BACKEND_URL is required when JUNJO_ENV=production"
    echo "See .env.example for configuration details"
    exit 1
  fi

  # Use explicit URLs
  API_HOST="$JUNJO_PROD_BACKEND_URL"

  # Auto-derive OTLP endpoint if not set
  if [ -z "$JUNJO_PROD_OTLP_ENDPOINT" ]; then
    # Extract host from backend URL and append :50051
    BACKEND_HOST=$(echo "$JUNJO_PROD_BACKEND_URL" | sed -E 's|https?://([^:/]+).*|\1|')
    OTLP_ENDPOINT="https://${BACKEND_HOST}:50051"
  else
    OTLP_ENDPOINT="$JUNJO_PROD_OTLP_ENDPOINT"
  fi
else
  # Development defaults
  API_HOST="http://localhost:1323"
  OTLP_ENDPOINT="grpc://localhost:50051"
fi

# Create the config file in the nginx web root
CONFIG_FILE="/usr/share/nginx/html/config.js"
echo "window.runtimeConfig = { API_HOST: \"${API_HOST}\" };" > $CONFIG_FILE

# === Startup Message ================================================================
GREEN='\033[0;32m'
BOLD='\033[1m'
NC='\033[0m' # No Color

printf "${BOLD}${GREEN}\n"
printf "  ----------------------------------\n\n"
printf "  🎏 Junjo AI Studio UI 🎏\n\n"
printf "  Environment: ${JUNJO_ENV:-development}\n"
printf "  API Host: ${API_HOST}\n"
if [ "$JUNJO_ENV" = "production" ]; then
  printf "  OTLP Endpoint: ${OTLP_ENDPOINT}\n"
fi
printf "\n  ----------------------------------\n\n"
printf "${NC}"

# The main Nginx entrypoint will continue executing after this script.
```

### Phase 2: Backend Configuration

#### 2.1. Update `backend/app/config/settings.py`

**Remove** (already done):
```python
junjo_prod_auth_domain: Annotated[str, ...]  # ✅ Already removed
```

**Add new fields to `SessionCookieSettings` class:**

```python
prod_frontend_url: Annotated[
    str | None,
    Field(
        default=None,
        description="Production frontend URL (e.g., https://app.example.com)",
        validation_alias="JUNJO_PROD_FRONTEND_URL",
    ),
]
prod_backend_url: Annotated[
    str | None,
    Field(
        default=None,
        description="Production backend URL (e.g., https://api.example.com)",
        validation_alias="JUNJO_PROD_BACKEND_URL",
    ),
]
prod_otlp_endpoint: Annotated[
    str | None,
    Field(
        default=None,
        description="Production OTLP ingestion endpoint (auto-derived from backend URL if not set)",
        validation_alias="JUNJO_PROD_OTLP_ENDPOINT",
    ),
]

@field_validator("prod_frontend_url", "prod_backend_url", mode="after")
@classmethod
def validate_production_urls(cls, v: str | None, info) -> str | None:
    """Validate production URLs are set when JUNJO_ENV=production."""
    junjo_env = info.data.get("junjo_env", "development")
    field_name = info.field_name

    if junjo_env == "production" and v is None:
        raise ValueError(
            f"{field_name.upper()} is required when JUNJO_ENV=production. "
            "See .env.example and docs/DEPLOYMENT.md for configuration details."
        )

    # Validate URL format if set
    if v is not None:
        if not v.startswith(("http://", "https://")):
            raise ValueError(
                f"{field_name} must start with http:// or https://. Got: {v}"
            )

    return v

@computed_field  # type: ignore[prop-decorator]
@property
def otlp_endpoint(self) -> str:
    """Computed OTLP endpoint (auto-derived from backend URL if not set)."""
    if self.junjo_env != "production":
        return "grpc://localhost:50051"

    # Use explicit override if provided
    if self.prod_otlp_endpoint:
        return self.prod_otlp_endpoint

    # Auto-derive from backend URL
    if self.prod_backend_url:
        from urllib.parse import urlparse
        parsed = urlparse(self.prod_backend_url)
        # Use same protocol as backend, append :50051
        return f"{parsed.scheme}://{parsed.netloc}:50051"

    # Fallback (shouldn't reach here due to validation)
    return "grpc://localhost:50051"
```

#### 2.2. Enhance `backend/app/main.py` Startup Validation

Replace lines 76-106 (startup validation section):

```python
# Validate deployment configuration
logger.info("-" * 60)
if settings.session_cookie.junjo_env == "production":
    # Production mode checks
    logger.info("🔒 Production mode detected")

    # URLs are already validated by Pydantic (will raise ValueError if missing)
    # Just display configuration
    logger.info(f"✅ Frontend URL: {settings.session_cookie.prod_frontend_url}")
    logger.info(f"✅ Backend URL: {settings.session_cookie.prod_backend_url}")
    logger.info(f"✅ OTLP Endpoint: {settings.session_cookie.otlp_endpoint}")

    # Auto-derive CORS origins if not explicitly set
    if not settings.cors_origins or settings.cors_origins == ["*"]:
        settings.cors_origins = [settings.session_cookie.prod_frontend_url]
        logger.info(f"✅ CORS origins (auto-derived): {settings.cors_origins}")
    else:
        logger.info(f"✅ CORS origins (explicit): {settings.cors_origins}")

    # Validate same-domain requirement for session cookies
    from urllib.parse import urlparse

    frontend_domain = urlparse(settings.session_cookie.prod_frontend_url).netloc
    backend_domain = urlparse(settings.session_cookie.prod_backend_url).netloc

    # Extract eTLD+1 (registrable domain) for comparison
    # Simplified: just compare last two parts (e.g., example.com)
    # Production code could use tldextract library for proper eTLD+1 extraction
    frontend_root = ".".join(frontend_domain.split(".")[-2:])
    backend_root = ".".join(backend_domain.split(".")[-2:])

    if frontend_root != backend_root:
        logger.error(
            f"❌ DEPLOYMENT ERROR: Frontend ({frontend_domain}) and Backend ({backend_domain}) "
            "must share the same root domain for session cookies to work.\n"
            f"   Frontend root: {frontend_root}\n"
            f"   Backend root: {backend_root}\n"
            "   See docs/DEPLOYMENT.md for supported deployment configurations."
        )
        raise ValueError(
            f"Frontend and backend must share same root domain. "
            f"Got: {frontend_root} vs {backend_root}"
        )

    logger.info(f"✅ Same-domain requirement validated ({frontend_root})")
    logger.info("✅ HTTPS-only cookies enabled")
    logger.info("✅ Session cookies: Encrypted (AES-256) + Signed (HMAC)")
    logger.info("✅ CSRF protection: SameSite=Strict")
    logger.info("")
    logger.info("⚠️  DEPLOYMENT REMINDER:")
    logger.info("   Backend and frontend must be accessible via configured URLs")
    logger.info("   Ensure reverse proxy/load balancer routes traffic correctly")
else:
    # Development mode
    logger.info("🔧 Development mode")
    logger.info("⚠️  HTTPS-only cookies disabled (development only)")
    logger.info("✅ Session cookies: Encrypted (AES-256) + Signed (HMAC)")
    logger.info("✅ CSRF protection: SameSite=Strict")
logger.info("-" * 60)
```

### Phase 3: Documentation Updates

#### 3.1. Update `AGENTS.md`

Update section 3.2 (Web UI Session Cookie Authentication):

```markdown
### Historical Note

**Removed Configuration:** An earlier design included `JUNJO_PROD_AUTH_DOMAIN` environment
variable for setting the `domain` parameter explicitly. This was removed because:

1. **Not necessary**: Browser SameSite behavior handles subdomain cookies correctly without it
2. **Added complexity**: Required users to configure production domain, risking misconfiguration
3. **Less secure**: Explicit domain parameter can widen cookie scope unnecessarily
4. **Simplified deployment**: Fewer environment variables to manage

**New Configuration (v2):** The system was later updated to use explicit URL configuration
instead of domain-based auto-construction:

- `JUNJO_PROD_FRONTEND_URL`: Explicit frontend URL (e.g., https://app.example.com)
- `JUNJO_PROD_BACKEND_URL`: Explicit backend URL (e.g., https://api.example.com)
- `JUNJO_PROD_OTLP_ENDPOINT`: Auto-derived from backend URL with override capability

This approach provides better flexibility while maintaining simplicity. See `.env.example`
for complete configuration details.
```

#### 3.2. Update `docs/DEPLOYMENT.md`

Update all configuration examples to use new variables:

**Before:**
```bash
JUNJO_PROD_AUTH_DOMAIN=example.com
```

**After:**
```bash
JUNJO_PROD_FRONTEND_URL=https://app.example.com
JUNJO_PROD_BACKEND_URL=https://api.example.com
# JUNJO_PROD_OTLP_ENDPOINT=https://api.example.com:50051  # Optional override
```

Add troubleshooting section:

```markdown
### Troubleshooting: Domain Configuration

**Error: "Frontend and backend must share same root domain"**

This error occurs when the frontend and backend URLs are on different domains, which breaks
session cookie authentication.

**Examples:**

❌ **Invalid (different domains):**
```bash
JUNJO_PROD_FRONTEND_URL=https://app.example.com
JUNJO_PROD_BACKEND_URL=https://service.run.app
```
Error: `example.com` vs `run.app` (different eTLD+1)

✅ **Valid (same domain):**
```bash
JUNJO_PROD_FRONTEND_URL=https://app.example.com
JUNJO_PROD_BACKEND_URL=https://api.example.com
```
Both share root domain: `example.com`

**Solution:** Deploy both services on subdomains of the same domain, or use a reverse proxy
to route both services through the same domain.
```

#### 3.3. Update `README.md`

Update Quick Start production configuration example:

```bash
# Production configuration in .env
JUNJO_ENV=production
JUNJO_PROD_FRONTEND_URL=https://app.example.com
JUNJO_PROD_BACKEND_URL=https://api.example.com
```

---

## Testing Plan

### Unit Tests

#### Test: URL Validation (`backend/app/config/test_settings.py`)

```python
def test_production_requires_frontend_url():
    """Test that production mode requires JUNJO_PROD_FRONTEND_URL."""
    with pytest.raises(ValueError, match="JUNJO_PROD_FRONTEND_URL is required"):
        SessionCookieSettings(
            junjo_env="production",
            secure_cookie_key="...",
            session_secret="...",
            # Missing: prod_frontend_url
            prod_backend_url="https://api.example.com",
        )

def test_production_requires_backend_url():
    """Test that production mode requires JUNJO_PROD_BACKEND_URL."""
    with pytest.raises(ValueError, match="JUNJO_PROD_BACKEND_URL is required"):
        SessionCookieSettings(
            junjo_env="production",
            secure_cookie_key="...",
            session_secret="...",
            prod_frontend_url="https://app.example.com",
            # Missing: prod_backend_url
        )

def test_url_must_have_protocol():
    """Test that URLs must start with http:// or https://."""
    with pytest.raises(ValueError, match="must start with http"):
        SessionCookieSettings(
            junjo_env="production",
            secure_cookie_key="...",
            session_secret="...",
            prod_frontend_url="app.example.com",  # Missing https://
            prod_backend_url="https://api.example.com",
        )

def test_otlp_endpoint_auto_derived():
    """Test that OTLP endpoint is auto-derived from backend URL."""
    settings = SessionCookieSettings(
        junjo_env="production",
        secure_cookie_key="...",
        session_secret="...",
        prod_frontend_url="https://app.example.com",
        prod_backend_url="https://api.example.com",
    )
    assert settings.otlp_endpoint == "https://api.example.com:50051"

def test_otlp_endpoint_override():
    """Test that OTLP endpoint can be explicitly overridden."""
    settings = SessionCookieSettings(
        junjo_env="production",
        secure_cookie_key="...",
        session_secret="...",
        prod_frontend_url="https://app.example.com",
        prod_backend_url="https://api.example.com",
        prod_otlp_endpoint="https://ingest.example.com:443",
    )
    assert settings.otlp_endpoint == "https://ingest.example.com:443"
```

#### Test: Same-Domain Validation (`backend/app/test_main.py`)

```python
def test_startup_validates_same_domain():
    """Test that startup validation catches cross-domain configuration."""
    # This would be tested by setting environment variables and checking
    # that the app raises ValueError on startup
    pass  # Implementation depends on test harness
```

### Integration Tests

#### Test: Frontend Startup Script

```bash
# Test: Production mode with missing variables
export JUNJO_ENV=production
./frontend/prod-startup.sh
# Expected: Exit code 1, error message about missing JUNJO_PROD_FRONTEND_URL

# Test: Production mode with valid config
export JUNJO_ENV=production
export JUNJO_PROD_FRONTEND_URL=https://app.example.com
export JUNJO_PROD_BACKEND_URL=https://api.example.com
./frontend/prod-startup.sh
# Expected: Exit code 0, config.js created with correct API_HOST

# Test: OTLP endpoint auto-derivation
cat /usr/share/nginx/html/config.js
# Expected: window.runtimeConfig = { API_HOST: "https://api.example.com" };

# Test: OTLP endpoint override
export JUNJO_PROD_OTLP_ENDPOINT=https://custom.example.com:443
./frontend/prod-startup.sh
# Expected: Startup message shows custom OTLP endpoint
```

#### Test: Backend Startup Validation

```bash
# Test: Production mode with missing URLs
JUNJO_ENV=production uvicorn app.main:app
# Expected: ValueError, clear error message

# Test: Production mode with cross-domain URLs
JUNJO_ENV=production \
JUNJO_PROD_FRONTEND_URL=https://app.example.com \
JUNJO_PROD_BACKEND_URL=https://service.run.app \
uvicorn app.main:app
# Expected: ValueError about different domains

# Test: Production mode with valid config
JUNJO_ENV=production \
JUNJO_PROD_FRONTEND_URL=https://app.example.com \
JUNJO_PROD_BACKEND_URL=https://api.example.com \
uvicorn app.main:app
# Expected: Successful startup, validation messages in logs
```

### Manual Testing Checklist

- [ ] Development mode works without production URLs
- [ ] Production mode requires both frontend and backend URLs
- [ ] Invalid URLs (missing protocol) are rejected
- [ ] Cross-domain URLs are rejected with clear error
- [ ] Same-domain URLs are accepted (subdomain + subdomain)
- [ ] Same-domain URLs are accepted (apex + subdomain)
- [ ] Same-domain URLs are accepted (subdomain + apex)
- [ ] OTLP endpoint auto-derives correctly
- [ ] OTLP endpoint override works
- [ ] CORS origins auto-derive from frontend URL
- [ ] Startup logs display all computed values
- [ ] Frontend config.js generated with correct API_HOST
- [ ] Frontend can successfully call backend API in production

---

## Migration Guide for Users

### For Existing Deployments Using Undocumented Variable

If you were using the undocumented `JUNJO_PROD_AUTH_DOMAIN` variable:

**Old Configuration:**
```bash
JUNJO_ENV=production
JUNJO_PROD_AUTH_DOMAIN=example.com
```

**New Configuration:**
```bash
JUNJO_ENV=production
JUNJO_PROD_FRONTEND_URL=https://example.com
JUNJO_PROD_BACKEND_URL=https://api.example.com
```

**Migration Steps:**

1. Update `.env` file with new variables
2. Remove `JUNJO_PROD_AUTH_DOMAIN` (no longer used)
3. Restart services: `docker compose down && docker compose up -d`
4. Verify startup logs show correct URLs

### For Custom Subdomain Patterns

If you use custom subdomains (not `api.`):

**Example: backend.example.com instead of api.example.com**

**New Configuration:**
```bash
JUNJO_ENV=production
JUNJO_PROD_FRONTEND_URL=https://app.example.com
JUNJO_PROD_BACKEND_URL=https://backend.example.com  # ✅ Now supported!
```

### For Non-Standard OTLP Ports

If your platform uses a different port (e.g., Cloud Run on 443):

**Configuration:**
```bash
JUNJO_ENV=production
JUNJO_PROD_FRONTEND_URL=https://app.example.com
JUNJO_PROD_BACKEND_URL=https://api.example.com
JUNJO_PROD_OTLP_ENDPOINT=https://api.example.com:443  # Override default :50051
```

---

## Benefits Summary

### For Users

✅ **Explicit Configuration**: No guessing what URLs will be constructed
✅ **Flexible Deployment**: Supports all subdomain patterns and apex domains
✅ **Self-Validating**: Catches configuration errors before deployment
✅ **Clear Errors**: Helpful messages when something is wrong
✅ **Works Everywhere**: Cloud Run, Railway, VPS, any platform

### For Maintainers

✅ **Fewer Support Issues**: Built-in validation prevents common mistakes
✅ **Better Documentation**: Clear, explicit configuration is self-documenting
✅ **Easier Testing**: Explicit URLs are easier to mock and test
✅ **Future-Proof**: Easy to extend with new endpoints or services

---

## Implementation Checklist

### Phase 1: Configuration & Frontend
- [ ] Update `.env.example` with new variables
- [ ] Refactor `frontend/prod-startup.sh`
- [ ] Test frontend startup script with various configs
- [ ] Update frontend deployment documentation

### Phase 2: Backend
- [ ] Remove `junjo_prod_auth_domain` from `settings.py` (✅ Done)
- [ ] Add new URL fields to `SessionCookieSettings`
- [ ] Add URL validation
- [ ] Add auto-derivation for OTLP endpoint
- [ ] Update startup validation in `main.py`
- [ ] Add same-domain validation
- [ ] Test backend startup with various configs

### Phase 3: Documentation
- [ ] Update `AGENTS.md` section 3.2
- [ ] Update `docs/DEPLOYMENT.md` with new variables
- [ ] Add troubleshooting section
- [ ] Update `README.md` Quick Start
- [ ] Create migration guide

### Phase 4: Testing
- [ ] Write unit tests for URL validation
- [ ] Write unit tests for auto-derivation
- [ ] Write integration tests for startup
- [ ] Manual testing checklist
- [ ] Test migration path from old config

### Phase 5: Cleanup
- [ ] Search codebase for any remaining references to `JUNJO_PROD_AUTH_DOMAIN`
- [ ] Update GitHub issue templates if needed
- [ ] Update deployment templates in example repos

---

## Rollout Strategy

### Version 1: Backward Compatible (Recommended)

Support both old and new configuration temporarily:

```bash
# frontend/prod-startup.sh - Phase 1
if [ "$JUNJO_ENV" = "production" ]; then
  # Try new variables first
  if [ -n "$JUNJO_PROD_FRONTEND_URL" ]; then
    API_HOST="$JUNJO_PROD_BACKEND_URL"
  # Fall back to old variable (with deprecation warning)
  elif [ -n "$JUNJO_PROD_AUTH_DOMAIN" ]; then
    echo "WARNING: JUNJO_PROD_AUTH_DOMAIN is deprecated. Use JUNJO_PROD_FRONTEND_URL and JUNJO_PROD_BACKEND_URL instead."
    API_HOST="https://api.${JUNJO_PROD_AUTH_DOMAIN}"
  else
    echo "ERROR: Production mode requires JUNJO_PROD_FRONTEND_URL and JUNJO_PROD_BACKEND_URL"
    exit 1
  fi
fi
```

**Timeline:**
- **v1.0**: Add new variables, support both old and new (with deprecation warning)
- **v1.1**: Remove old variable support

### Version 2: Breaking Change (Simpler)

Go straight to new configuration:

```bash
# frontend/prod-startup.sh - Immediate breaking change
if [ "$JUNJO_ENV" = "production" ]; then
  if [ -z "$JUNJO_PROD_FRONTEND_URL" ] || [ -z "$JUNJO_PROD_BACKEND_URL" ]; then
    echo "ERROR: Production mode requires JUNJO_PROD_FRONTEND_URL and JUNJO_PROD_BACKEND_URL"
    exit 1
  fi
  API_HOST="$JUNJO_PROD_BACKEND_URL"
fi
```

**Recommendation**: Use **Version 2 (breaking change)** because:
1. `JUNJO_PROD_AUTH_DOMAIN` was never documented (limited impact)
2. Simpler codebase (no legacy support)
3. Clear migration path in CHANGELOG

---

## Success Criteria

### Configuration Works When:

✅ Development mode works without production variables
✅ Production mode requires explicit URLs
✅ Invalid URLs are rejected with clear errors
✅ Cross-domain deployments are blocked at startup
✅ Same-domain deployments work (all patterns)
✅ OTLP endpoint auto-derives correctly
✅ OTLP endpoint can be overridden
✅ CORS origins auto-derive from frontend URL
✅ Startup logs display all computed values

### Documentation Is Complete When:

✅ `.env.example` documents all variables
✅ `docs/DEPLOYMENT.md` has updated examples
✅ Migration guide exists for old variable
✅ Troubleshooting section covers common errors
✅ `AGENTS.md` explains configuration architecture

### Testing Is Complete When:

✅ Unit tests cover validation logic
✅ Unit tests cover auto-derivation
✅ Integration tests verify startup behavior
✅ Manual testing checklist completed
✅ Migration path tested

---

## Open Questions

1. **Should we add `tldextract` library for proper eTLD+1 extraction?**
   - Current: Simple string split (last 2 parts)
   - Pro: More accurate (handles `.co.uk`, etc.)
   - Con: Additional dependency

2. **Should we add a `/config` endpoint for frontend to fetch OTLP endpoint dynamically?**
   - Current: OTLP endpoint only shown in startup logs
   - Pro: Frontend can display it in API keys page
   - Con: Additional endpoint, more complexity

3. **Should we validate that OTLP endpoint shares same domain as backend?**
   - Current: No validation (allows flexibility)
   - Pro: Prevents misconfiguration
   - Con: Some platforms might use different OTLP ingestion URL

---

## Notes

- This plan was created based on analysis of current codebase state as of 2025-01-15
- The `JUNJO_PROD_AUTH_DOMAIN` variable was found to be used but undocumented
- Session cookies already work without domain parameter due to SameSite behavior
- The new configuration provides better flexibility while maintaining simplicity
- Implementation priority: configuration files → backend → documentation → testing

---

**Plan Status**: Ready for review and approval
**Next Steps**: Review plan, approve, and begin implementation
