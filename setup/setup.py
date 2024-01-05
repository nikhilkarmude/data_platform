import os
import subprocess

# Flag to control virtual environment creation
VENV_FLAG = False

# Name your virtual environment
VENV_NAME = "cdp-venv"

if VENV_FLAG:
    # Create the virtual environment
    subprocess.call(f'python -m venv {VENV_NAME}', shell=True)

    # Activate the virtual environment
    activate_venv = os.path.join(VENV_NAME, 'Scripts', 'activate')
    # Use exec to replace the current process with the activated venv
    exec(open(activate_venv).read(), dict(__file__=activate_venv))

# Upgrade pip
subprocess.call('pip install --upgrade pip', shell=True)

# Install the libraries using requirements.txt
subprocess.call('pip install -r requirements.txt', shell=True)

if VENV_FLAG:
    # Verify the installed libraries
    subprocess.call('pip freeze', shell=True)

    # Deactivate the virtual environment
    # This isn't strictly necessary in the script context, since the script's environment will be automatically deactivated when the script finishes
    # The line below is included just for completeness
    exec('deactivate')
