#!/bin/bash

# Script to run the ERPNext dummy data generator
# Place this in your project root directory

echo "=================================="
echo "ERPNext Dummy Data Generator"
echo "=================================="

# Check if the Python script exists
if [ ! -f "generate_dummy_data.py" ]; then
    echo "Error: generate_dummy_data.py not found!"
    echo "Please ensure the Python script is in the current directory."
    exit 1
fi

# Install required Python packages if not already installed
echo "Installing required Python packages..."
pip install faker requests

# Option 1: Run directly (if ERPNext API is accessible from host)
echo ""
echo "Running data generator..."
python3 generate_dummy_data.py

# Option 2: If you need to run inside a Docker container, uncomment below:
# echo "Running inside ERPNext container..."
# docker exec -it erpnext_backend_1 python /path/to/generate_dummy_data.py

echo ""
echo "Script execution completed!"
echo "Check erpnext_data_generation.log for details."