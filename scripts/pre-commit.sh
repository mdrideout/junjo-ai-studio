#!/bin/bash
# Pre-commit hook to auto-generate proto files
# This ensures proto files are always up-to-date before committing

set -e

echo "🔄 Pre-commit: Regenerating proto files..."

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the repository root
REPO_ROOT=$(git rev-parse --show-toplevel)

# Track if any proto files were modified
PROTO_MODIFIED=false

# Function to generate Go proto files for ingestion-service
generate_go_proto() {
  echo "  → Generating Go proto files for ingestion-service..."
  cd "$REPO_ROOT/ingestion-service"

  # Check if protoc is available
  if ! command -v protoc &> /dev/null; then
    echo -e "${YELLOW}⚠️  Warning: protoc not found. Skipping Go proto generation.${NC}"
    echo "     Install protoc: https://grpc.io/docs/protoc-installation/"
    return 1
  fi

  # Check if Go plugins are available
  if ! command -v protoc-gen-go &> /dev/null || ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo -e "${YELLOW}⚠️  Warning: Go protobuf plugins not found. Skipping Go proto generation.${NC}"
    echo "     Install with: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"
    echo "                   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"
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
