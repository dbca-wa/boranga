import os
import subprocess
import time

# Configuration
WATCH_DIRS = ["boranga"]
EXTENSIONS = {".py"}
POLL_INTERVAL = 1.0


def get_file_mtimes():
    file_mtimes = {}
    for watch_dir in WATCH_DIRS:
        if not os.path.exists(watch_dir):
            continue
        for root, _, files in os.walk(watch_dir):
            for f in files:
                if any(f.endswith(ext) for ext in EXTENSIONS):
                    path = os.path.join(root, f)
                    try:
                        mtime = os.path.getmtime(path)
                        file_mtimes[path] = mtime
                    except OSError:
                        pass
    return file_mtimes


def run_ruff(filepath):
    # Run ruff check --fix and ruff format
    # We ignore errors to keep the watcher alive
    try:
        subprocess.run(["ruff", "check", "--fix", filepath], check=False, capture_output=True)
        subprocess.run(["ruff", "format", filepath], check=False, capture_output=True)
        print(f"Formatted: {filepath}")
    except Exception as e:
        print(f"Error formatting {filepath}: {e}")


def main():
    print("Starting Auto-Ruff Watcher...")
    print(f"Watching: {WATCH_DIRS}")

    # Initial scan
    last_mtimes = get_file_mtimes()

    while True:
        time.sleep(POLL_INTERVAL)
        current_mtimes = get_file_mtimes()

        for filepath, mtime in current_mtimes.items():
            if filepath not in last_mtimes or mtime > last_mtimes[filepath]:
                # File changed
                # Check if it was us modifying it (debounce?)
                # Ruff usually preserves mtime or updates it.
                # To avoid infinite loop, we update last_mtime immediately after.
                # But actually, if ruff modifies it, mtime updates again.
                # So we might double-format. That's fine, ruff is idempotent.

                # However, if we format, mtime changes -> loop triggers again -> clean -> no change -> mtime doesn't change?
                # No, ruff format writes new file only if changed?
                # If ruff doesn't change file, does it touch mtime?
                # Ideally no.

                run_ruff(filepath)

        last_mtimes = get_file_mtimes()


if __name__ == "__main__":
    main()
