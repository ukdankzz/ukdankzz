#!/bin/bash

# UKDANKZZ Bot 24/7 Startup Script
# This ensures the bot runs continuously on Reserved VM

echo "🚀 UKDANKZZ Bot 24/7 Startup Script"
echo "🏠 Running on Reserved VM for true 24/7 operation"

# Set environment variables for reliability
export PYTHONUNBUFFERED=1
export PYTHONDONTWRITEBYTECODE=1

# Use polling mode - conflict fixed by preventing aiogram retries
export BOT_RUNTIME=dev
echo "💻 POLLING mode enabled"

# Function to start the bot with auto-restart
start_bot() {
    local attempt=1
    local max_attempts=999999  # Infinite attempts for 24/7 operation
    
    while [ $attempt -le $max_attempts ]; do
        echo "🔄 Starting bot attempt $attempt"
        echo "⏰ $(date): Starting UKDANKZZ Bot..."
        
        # Start the bot with safety flag to prevent .replit entrypoint from running it
        export BOT_START_AUTHORIZED=1
        python main.py
        
        # If we reach here, the bot stopped
        local exit_code=$?
        echo "⚠️ $(date): Bot stopped with exit code $exit_code"
        
        # Wait 10 seconds before restart to prevent rapid restart loops
        echo "⏳ Waiting 10 seconds before restart..."
        sleep 10
        
        # Clean up any lock files from crashed processes
        rm -f /tmp/ukdankzz_bot.lock
        echo "🧹 Cleaned up lock files"
        
        attempt=$((attempt + 1))
    done
}

# Trap signals to clean up on script exit
cleanup() {
    echo "🛑 Startup script stopping..."
    rm -f /tmp/ukdankzz_bot.lock
    exit 0
}

trap cleanup SIGINT SIGTERM

echo "🎯 Starting infinite restart loop for 24/7 operation..."
start_bot