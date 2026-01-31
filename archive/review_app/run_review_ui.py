#!/usr/bin/env python3
"""
Background runner for the review UI with auto-reload on file changes.

This script runs the Flask app in the background and automatically
restarts it when code changes are detected.
"""

import subprocess
import sys
import time
import signal
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class CodeChangeHandler(FileSystemEventHandler):
    """Handler for file system changes."""

    def __init__(self, restart_callback):
        self.restart_callback = restart_callback
        self.last_restart = 0

    def on_modified(self, event):
        # Ignore directory changes and non-Python/HTML files
        if event.is_directory:
            return

        path = Path(event.src_path)
        if path.suffix not in ['.py', '.html']:
            return

        # Debounce - don't restart more than once per 2 seconds
        now = time.time()
        if now - self.last_restart < 2:
            return

        print(f"\n[CHANGE DETECTED] {path.name} modified, restarting...")
        self.last_restart = now
        self.restart_callback()

class FlaskRunner:
    """Manages the Flask process."""

    def __init__(self):
        self.process = None
        self.log_file = None
        self.should_run = True

    def start(self):
        """Start the Flask app."""
        if self.process:
            self.stop()

        # Open log file (in parent directory)
        log_path = Path(__file__).parent.parent / "review_ui.log"
        self.log_file = open(log_path, 'a')
        self.log_file.write(f"\n\n=== Starting Flask app at {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        self.log_file.flush()

        # Start process (review_ui.py is in same directory)
        app_path = Path(__file__).parent / "review_ui.py"
        self.process = subprocess.Popen(
            ["uv", "run", str(app_path)],
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            cwd=Path(__file__).parent
        )

        print(f"[STARTED] Flask app running (PID: {self.process.pid})")

    def stop(self):
        """Stop the Flask app."""
        if self.process:
            print("[STOPPING] Shutting down Flask app...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            print("[STOPPED] Flask app shut down")

        if self.log_file:
            self.log_file.close()

    def restart(self):
        """Restart the Flask app."""
        self.stop()
        time.sleep(0.5)
        self.start()

    def run(self):
        """Main run loop."""
        print("=" * 70)
        print("Photo Review UI - Background Runner")
        print("=" * 70)
        print()
        print("The Flask app will start in the background and auto-reload on changes.")
        print()
        print("UI available at: http://localhost:5000")
        print("Logs written to: review_ui.log")
        print()
        print("Press Ctrl+C to stop")
        print("=" * 70)
        print()

        # Start Flask
        self.start()

        # Set up file watching (watch current directory)
        review_app_dir = Path(__file__).parent
        event_handler = CodeChangeHandler(self.restart)
        observer = Observer()
        observer.schedule(event_handler, str(review_app_dir), recursive=True)
        observer.start()

        print("[WATCHING] Monitoring for file changes...")
        print()

        try:
            # Keep running
            while self.should_run:
                # Check if process is still alive
                if self.process.poll() is not None:
                    print("[ERROR] Flask app crashed! Check review_ui.log for details.")
                    print("[RESTARTING] Attempting to restart...")
                    self.start()

                time.sleep(1)

        except KeyboardInterrupt:
            print("\n[SHUTDOWN] Received interrupt signal...")

        finally:
            observer.stop()
            observer.join()
            self.stop()
            print("[EXIT] Background runner stopped")

def main():
    runner = FlaskRunner()
    runner.run()

if __name__ == '__main__':
    main()
