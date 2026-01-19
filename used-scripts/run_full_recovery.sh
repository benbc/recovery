#!/bin/bash
# Run full RECOVERY directory scan in background

cd /home/ben/src/benbc/recovery

# Backup existing results if any
if [ -d "organized" ]; then
    echo "Backing up existing organized/ directory..."
    mv organized organized.backup.$(date +%s)
fi

# Run with nohup so it survives disconnection
echo "Starting recovery script on full directory (~40 minutes estimated)..."
echo "Monitor progress with: tail -f recovery_full.log"
echo "Or check database: python3 -c \"import sqlite3; conn = sqlite3.connect('organized/photos.db'); print('Photos:', conn.execute('SELECT COUNT(*) FROM photos').fetchone()[0])\""
echo ""

nohup ./recovery > recovery_full.log 2>&1 &

PID=$!
echo "Started with PID: $PID"
echo "To check if still running: ps -p $PID"
echo "To stop: kill $PID"
