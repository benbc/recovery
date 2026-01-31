#!/bin/bash
# Start the duplicate photo review UI with auto-reload

cd "$(dirname "$0")/review_app"
chmod +x run_review_ui.py

# Run the background runner
uv run --with watchdog run_review_ui.py
