#!/bin/bash

# Script to set up a Python 3.10 virtual environment and install requirements

# Exit immediately if a command exits with a non-zero status
set -e

# Configuration
PYTHON_VERSION="python3.10"
VENV_DIR=".venv"
REQUIREMENTS_FILE="requirements.txt"

# Check if Python 3.10 is installed
if ! command -v $PYTHON_VERSION &> /dev/null; then
  echo "Error: $PYTHON_VERSION is not installed. Please install it before proceeding." >&2
  exit 1
fi

# Create the virtual environment
if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment in directory: $VENV_DIR"
  $PYTHON_VERSION -m venv $VENV_DIR
else
  echo "Virtual environment already exists in directory: $VENV_DIR"
fi

# Activate the virtual environment
source $VENV_DIR/bin/activate

# Upgrade pip in the virtual environment
echo "Upgrading pip..."
pip install --upgrade pip

# Install requirements
if [ -f "$REQUIREMENTS_FILE" ]; then
  echo "Installing requirements from $REQUIREMENTS_FILE..."
  pip install -r $REQUIREMENTS_FILE
else
  echo "Warning: $REQUIREMENTS_FILE not found. Skipping requirements installation."
fi

echo "Setup complete. To activate the virtual environment, run:"
echo "source $VENV_DIR/bin/activate"
