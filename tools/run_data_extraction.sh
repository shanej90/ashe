#!/bin/bash

set -e  # Exit immediately if a command fails
set -o pipefail

echo "🔧 Starting ETL script runner..."

# Get the directory this script lives in (absolute path)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "📁 Script directory: $SCRIPT_DIR"

# Go up one level to get the project root
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
echo "📂 Project root: $PROJECT_ROOT"

# Check that the virtual environment exists
VENV_PATH="$PROJECT_ROOT/.venv"
if [[ ! -d "$VENV_PATH" ]]; then
  echo "❌ ERROR: Virtual environment not found at $VENV_PATH"
  exit 1
fi

# Activate virtual environment
echo "🐍 Activating virtual environment..."
source "$VENV_PATH/Scripts/activate"

# Check that the Python script exists
PYTHON_SCRIPT="$PROJECT_ROOT/run_pipeline/run_data_extraction.py"
if [[ ! -f "$PYTHON_SCRIPT" ]]; then
  echo "❌ ERROR: Python script not found at $PYTHON_SCRIPT"
  exit 1
fi

# Run the script using the module approach (so imports work)
echo "🚀 Running ETL process via module..."
cd "$PROJECT_ROOT"
python -m run_pipeline.run_data_extraction

EXIT_CODE=$?
if [[ $EXIT_CODE -ne 0 ]]; then
  echo "❌ ETL script failed with exit code $EXIT_CODE"
  exit $EXIT_CODE
else
  echo "✅ ETL script completed successfully."
fi
