#!/bin/bash

# Define the virtual environment and requirements file paths
REPO_ROOT=$(pwd)
VENV_PATH="$REPO_ROOT/venv"
REQUIREMENTS_FILE="$REPO_ROOT/requirements-dev.txt"

# Check if virtual environment exists
if [ ! -d "$VENV_PATH" ]; then
    echo "Virtual environment not found. Please ensure it exists at: $VENV_PATH"
    exit 1
fi

# Activate the virtual environment
echo "Activating virtual environment..."
source "$VENV_PATH/bin/activate"

# Check if requirements-dev.txt exists
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "requirements-dev.txt not found at: $REQUIREMENTS_FILE"
    exit 1
fi

# Install dependencies
echo "Installing dependencies from requirements-dev.txt..."
pip install -r "$REQUIREMENTS_FILE"

echo "Bootstrap process complete."
