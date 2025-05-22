import os
import shutil
import subprocess

# Configuration
PYTHON_EXECUTABLE = "python"  # Change this if needed
OUTPUT_DIR = "dist"
BUILD_DIR = "build"
SPEC_FILE = "sync.spec"

# Ensure PyInstaller is installed
print("Checking PyInstaller installation...")
try:
    subprocess.run([PYTHON_EXECUTABLE, "-m", "pip",
                   "install", "pyinstaller"], check=True)
    print("PyInstaller is installed.")
except subprocess.CalledProcessError:
    print("Failed to install PyInstaller. Please install manually.")
    exit(1)

# Ensure required packages are installed
print("Installing required packages...")
try:
    subprocess.run([PYTHON_EXECUTABLE, "-m", "pip", "install",
                   "pyodbc", "requests"], check=True)
    print("Required packages installed.")
except subprocess.CalledProcessError:
    print("Failed to install required packages. Please install manually.")
    exit(1)

# Clean previous build
print("Cleaning previous build...")
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
if os.path.exists(BUILD_DIR):
    shutil.rmtree(BUILD_DIR)
if os.path.exists(SPEC_FILE):
    os.remove(SPEC_FILE)

# Build executable with PyInstaller
print("Building executable...")
try:
    subprocess.run([
        PYTHON_EXECUTABLE,
        "-m",
        "PyInstaller",
        "--onefile",
        "--noconsole",
        "--add-data", "config.json;.",
        "sync.py"
    ], check=True)
    print("Build completed successfully.")
except subprocess.CalledProcessError:
    print("Build failed.")
    exit(1)

# Create distribution folder
print("Creating distribution package...")
os.makedirs("sync_tool", exist_ok=True)

# Copy files to distribution folder
shutil.copy(os.path.join(OUTPUT_DIR, "sync.exe"), "sync_tool")
shutil.copy("config.json", "sync_tool")
shutil.copy("sync.bat", "sync_tool")

# Create an empty log file
with open(os.path.join("sync_tool", "sync.log"), "w") as f:
    f.write("")

# Create README file
with open(os.path.join("sync_tool", "README.txt"), "w") as f:
    f.write("""DATABASE SYNC TOOL
=================

This tool synchronizes data from your local SQL Anywhere database to the web database.
NOTE: This tool will CLEAR existing data in the web database before adding new data.

SETUP:
1. Edit the config.json file to set your database connection details and API information
2. Double-click on sync.bat to run the synchronization
3. The window will close automatically when successful

REQUIREMENTS:
- ODBC Data Source for your SQL Anywhere database must be configured
- Internet connection to reach the web API

For support, contact: info@imcbsglobal.com
""")

print("Distribution package created successfully in the 'sync_tool' folder.")
print("You can now distribute the 'sync_tool' folder to users.")
