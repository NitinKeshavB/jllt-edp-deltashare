#!/usr/bin/env python3
"""
Regenerate requirements.txt from the current virtual environment.

This script exports all installed packages with their versions,
excluding the local editable package, and writes them to requirements.txt.

Usage:
    python scripts/regenerate_requirements.py
"""

import subprocess
import sys
from pathlib import Path

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.parent


def regenerate_requirements() -> int:
    """
    Regenerate requirements.txt from pip freeze output.

    Returns
    -------
    int
        Exit code (0 for success, 1 for failure)
    """
    requirements_path = PROJECT_ROOT / "requirements.txt"

    try:
        # Run pip freeze to get all installed packages
        result = subprocess.run(
            [sys.executable, "-m", "pip", "freeze"],
            capture_output=True,
            text=True,
            check=True,
        )

        # Filter out the local editable package and empty lines
        lines = []
        for line in result.stdout.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            # Skip editable installs of this package (any format)
            if "deltashare_api" in line.lower() or "deltashare-api" in line.lower():
                continue
            # Skip file:// paths (local editable installs)
            if "@ file://" in line:
                continue
            # Skip git-based editable installs
            if line.startswith("-e git+"):
                continue
            lines.append(line)

        # Sort lines alphabetically (case-insensitive)
        lines.sort(key=lambda x: x.lower())

        # Add the local package as editable install at the end
        lines.append("")
        lines.append("# Local package (editable install)")
        lines.append("-e .[dbrx,api,azure]")
        lines.append("")

        # Write to requirements.txt
        with open(requirements_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        print(f"✓ Successfully regenerated {requirements_path}")
        return 0

    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to run pip freeze: {e}", file=sys.stderr)
        return 1
    except IOError as e:
        print(f"✗ Failed to write requirements.txt: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(regenerate_requirements())
