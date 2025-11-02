#!/bin/bash
# Pre-commit hook to auto-generate proto files
# This ensures proto files are always up-to-date before committing

set -e

echo "🔄 Pre-commit: Regenerating proto files..."

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Get the repository root
REPO_ROOT=$(git rev-parse --show-toplevel)

# Track if any proto files were modified
PROTO_MODIFIED=false

# Required tool versions (must match PROTO_VERSIONS.md)
REQUIRED_PROTOC_VERSION="30.2"
REQUIRED_PROTOC_GEN_GO_VERSION="v1.36.10"
REQUIRED_PROTOC_GEN_GO_GRPC_VERSION="1.5.1"

# Function to check tool versions
check_tool_versions() {
  local version_mismatch=false

  # Check protoc version
  if command -v protoc &> /dev/null; then
    local protoc_version=$(protoc --version | awk '{print $2}')
    if [ "$protoc_version" != "$REQUIRED_PROTOC_VERSION" ]; then
      echo -e "${YELLOW}⚠️  Warning: protoc version mismatch${NC}"
      echo "     Expected: v${REQUIRED_PROTOC_VERSION}"
      echo "     Found:    v${protoc_version}"
      version_mismatch=true
    fi
  fi

  # Check protoc-gen-go version
  if command -v protoc-gen-go &> /dev/null; then
    local protoc_gen_go_version=$(protoc-gen-go --version 2>&1 | awk '{print $2}')
    if [ "$protoc_gen_go_version" != "$REQUIRED_PROTOC_GEN_GO_VERSION" ]; then
      echo -e "${YELLOW}⚠️  Warning: protoc-gen-go version mismatch${NC}"
      echo "     Expected: ${REQUIRED_PROTOC_GEN_GO_VERSION}"
      echo "     Found:    ${protoc_gen_go_version}"
      version_mismatch=true
    fi
  fi

  # Check protoc-gen-go-grpc version
  if command -v protoc-gen-go-grpc &> /dev/null; then
    local protoc_gen_go_grpc_version=$(protoc-gen-go-grpc --version 2>&1 | awk '{print $2}')
    if [ "$protoc_gen_go_grpc_version" != "$REQUIRED_PROTOC_GEN_GO_GRPC_VERSION" ]; then
      echo -e "${YELLOW}⚠️  Warning: protoc-gen-go-grpc version mismatch${NC}"
      echo "     Expected: ${REQUIRED_PROTOC_GEN_GO_GRPC_VERSION}"
      echo "     Found:    ${protoc_gen_go_grpc_version}"
      version_mismatch=true
    fi
  fi

  if [ "$version_mismatch" = true ]; then
    echo ""
    echo -e "${YELLOW}📚 To install correct versions, see: PROTO_VERSIONS.md${NC}"
    echo ""
  fi
}

# Check versions before generating
check_tool_versions

# Function to generate Go proto files for ingestion-service
generate_go_proto() {
  echo "  → Generating Go proto files for ingestion-service..."
  cd "$REPO_ROOT/ingestion-service"

  # Check if protoc is available
  if ! command -v protoc &> /dev/null; then
    echo -e "${YELLOW}⚠️  Warning: protoc not found. Skipping Go proto generation.${NC}"
    echo "     See PROTO_VERSIONS.md for installation instructions"
    return 1
  fi

  # Check if Go plugins are available
  if ! command -v protoc-gen-go &> /dev/null || ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo -e "${YELLOW}⚠️  Warning: Go protobuf plugins not found. Skipping Go proto generation.${NC}"
    echo "     See PROTO_VERSIONS.md for installation instructions"
    return 1
  fi

  # Generate proto files
  make proto > /dev/null 2>&1

  # Check if any files were modified
  if ! git diff --quiet proto_gen/; then
    PROTO_MODIFIED=true
    git add proto_gen/
    echo -e "  ${GREEN}✓${NC} Go proto files regenerated and staged"
  else
    echo "  ✓ Go proto files already up-to-date"
  fi
}

# Function to generate Python proto files for backend
generate_python_proto() {
  echo "  → Generating Python proto files for backend..."
  cd "$REPO_ROOT/backend"

  # Check if the generation script exists
  if [ ! -f "scripts/generate_proto.sh" ]; then
    echo -e "${YELLOW}⚠️  Warning: generate_proto.sh not found. Skipping Python proto generation.${NC}"
    return 1
  fi

  # Generate proto files
  ./scripts/generate_proto.sh > /dev/null 2>&1

  # Check if any files were modified
  if ! git diff --quiet app/proto_gen/; then
    PROTO_MODIFIED=true
    git add app/proto_gen/
    echo -e "  ${GREEN}✓${NC} Python proto files regenerated and staged"
  else
    echo "  ✓ Python proto files already up-to-date"
  fi
}

# Run proto generation for both services
generate_go_proto || true      # Continue even if Go proto generation fails
generate_python_proto || true  # Continue even if Python proto generation fails

# Summary
echo ""
if [ "$PROTO_MODIFIED" = true ]; then
  echo -e "${GREEN}✓ Proto files regenerated and staged for commit${NC}"
else
  echo -e "✓ All proto files are up-to-date"
fi

# Return to repo root
cd "$REPO_ROOT"

exit 0
