#!/bin/bash

# Script to properly run tutor backup/restore from crontab
# Usage: ./tutor_backup_wrapper.sh [backup|restore] [additional args]

# Determine user from environment or default to ubuntu
USER=${BACKUP_USER:-ubuntu}

# Validate user
if [[ "$USER" != "ubuntu" && "$USER" != "sambaash" ]]; then
    echo "Error: USER must be 'ubuntu' or 'sambaash', got: $USER"
    exit 1
fi

# Define paths
SCRIPT_DIR="/home/$USER/.local/share/tutor"
ENV_FILE="$SCRIPT_DIR/.env"
LOG_FILE="$SCRIPT_DIR/tutor_backup.log"
BACKUP_SCRIPT="$SCRIPT_DIR/tutor_backup_full.py"
RESTORE_SCRIPT="$SCRIPT_DIR/tutor_restore.py"

# Function for timestamped logging
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Immediately log the start
log "-------------------------------------------"
log "Starting backup wrapper script for user: $USER"

# Set home directory explicitly (important for cron)
export HOME="/home/$USER"
log "HOME set to $HOME"

# Source environment variables
if [ -f $HOME/.bashrc ]; then
    log "Sourcing .bashrc"
    source $HOME/.bashrc
fi

if [ -f $HOME/.profile ]; then
    log "Sourcing .profile"
    source $HOME/.profile
fi

# Initialize pyenv explicitly
export PATH="$HOME/.pyenv/bin:$PATH"
if command -v pyenv 1>/dev/null 2>&1; then
    log "Initializing pyenv"
    eval "$(pyenv init -)"
    eval "$(pyenv virtualenv-init -)"
else
    log "Warning: pyenv command not found"
fi

# Activate the ol_env virtual environment
log "Attempting to activate ol_env virtual environment"
if pyenv activate ol_env 2>/dev/null; then
    log "Successfully activated ol_env"
elif pyenv shell ol_env 2>/dev/null; then
    log "Successfully set ol_env using pyenv shell"
else
    log "Warning: Could not activate ol_env, trying to continue anyway"
fi

# Update PATH to include pyenv paths
export PATH="$HOME/.pyenv/shims:$HOME/.pyenv/bin:$PATH"
log "Updated PATH: $PATH"

# Now check for tutor command
TUTOR_BIN=$(which tutor 2>/dev/null || echo "")
log "Tutor command location: $TUTOR_BIN"

if [ -z "$TUTOR_BIN" ]; then
    log "ERROR: Could not find tutor command even after activating pyenv environment"
    exit 1
fi

# Get tutor root directory
TUTOR_ROOT=""
if [ -n "$TUTOR_BIN" ]; then
    TUTOR_ROOT=$(tutor config printroot 2>/dev/null || echo "")
    log "Tutor root directory: $TUTOR_ROOT"
fi

# Check Python and required packages
PYTHON_BIN=$(which python3 2>/dev/null || echo "")
log "Python binary: $PYTHON_BIN"

if [ -n "$PYTHON_BIN" ]; then
    # Check if required packages are installed in the current environment
    missing_packages=()
    if ! $PYTHON_BIN -c "import boto3" 2>/dev/null; then
        missing_packages+=("boto3")
    fi
    if ! $PYTHON_BIN -c "import google.cloud.storage" 2>/dev/null; then
        missing_packages+=("google-cloud-storage")
    fi
    if ! $PYTHON_BIN -c "import dotenv" 2>/dev/null; then
        missing_packages+=("python-dotenv")
    fi

    if [ ${#missing_packages[@]} -gt 0 ]; then
        log "Installing missing Python packages in ol_env: ${missing_packages[*]}"
        for package in "${missing_packages[@]}"; do
            log "Installing package: $package"
            $PYTHON_BIN -m pip install "$package"
        done
        log "Python packages installed"
    else
        log "All required Python packages are already installed in ol_env"
    fi
fi

# Check if .env file exists
if [ ! -f "$ENV_FILE" ]; then
    log "Error: .env file not found at $ENV_FILE"
    exit 1
else
    log ".env file found at $ENV_FILE"
fi

export COMPOSE_INTERACTIVE_NO_CLI=1
export DOCKER_CLI_HINTS=false
export COMPOSE_DOCKER_CLI_BUILD=1

# Determine which script to run
if [ "$1" == "backup" ]; then
    log "Running backup script..."
    # Redirect stdin from /dev/null to ensure no TTY
    DOTENV_PATH="$ENV_FILE" python3 "$BACKUP_SCRIPT" "${@:2}" < /dev/null >> "$LOG_FILE" 2>&1
elif [ "$1" == "restore" ]; then
    log "Running restore script..."
    # Redirect stdin from /dev/null to ensure no TTY
    DOTENV_PATH="$ENV_FILE" python3 "$RESTORE_SCRIPT" "${@:2}" < /dev/null >> "$LOG_FILE" 2>&1
else
    log "Error: Please specify either 'backup' or 'restore' as the first argument"
    exit 1
fi

# Log completion
log "Completed wrapper script"
log "-------------------------------------------"