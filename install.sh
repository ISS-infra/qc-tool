#!/bin/bash

# Install Python 3.9 if not already installed
if ! command -v python3.9 &> /dev/null; then
    echo "Python 3.9 is not installed. Installing..."
    sudo apt update
    sudo apt install python3.9
fi

# Install pip for Python 3.9
sudo apt install python3.9-distutils
wget https://bootstrap.pypa.io/get-pip.py
sudo python3.9 get-pip.py
rm get-pip.py

# Install libraries from requirements.txt
sudo python3.9 -m pip install -r requirements.txt

echo "Installation complete."
