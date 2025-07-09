#!/bin/bash

# Set the name of the virtual environment directory
VENV_DIR=".venv"

echo "🔧 Setting up virtual environment in $VENV_DIR..."

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv $VENV_DIR
    echo "✅ Virtual environment created."
else
    echo "🔁 Virtual environment already exists."
fi

# Activate virtual environment (works in interactive shells)
source $VENV_DIR/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📦 Installing packages from requirements.txt..."
pip install -r requirements.txt

echo "🎉 Setup complete. Environment is ready!"
