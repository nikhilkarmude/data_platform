#!/bin/bash

# Flag to control virtual environment creation
VENV_FLAG=true

# Name your virtual environment
VENV_NAME="dp-venv"

if $VENV_FLAG ; then
    # Create the virtual environment
    python3 -m venv $VENV_NAME

    # Activate the virtual environment
    source $VENV_NAME/bin/activate
fi

# Upgrade pip
pip3 install --upgrade pip

# Install the libraries using requirements.txt
pip3 install -r setup/requirements.txt

if $VENV_FLAG ; then
    # Verify the installed libraries
    pip3 freeze

    # Deactivate the virtual environment
    deactivate
fi
