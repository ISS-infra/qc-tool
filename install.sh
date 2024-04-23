# Install Python 3.9 if not already installed
if ! command -v python3.9 &> /dev/null; then
    echo "Python 3.9 is not installed. Please download and install Python 3.9 from https://www.python.org/downloads/"
    exit 1
fi

# Install pip for Python 3.9
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3.9 get-pip.py
del get-pip.py

# Install libraries from requirements.txt
python3.9 -m pip install -r requirements.txt

echo "Installation complete."
