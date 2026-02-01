# Claude Instructions for Photo Recovery Pipeline

## Required Reading

Before doing any work on this project, read these files:
1. **PLAN.md** - Architecture and design decisions (MUST follow)
2. **TODO.md** - Current work items and pending tasks
3. **RULES.md** - Documentation of all classification rules

## Key Operational Requirements

### Running Pipeline Stages

Always use the wrapper script (handles logging automatically):
```bash
./run --stage N        # Run single stage
./run --from-stage N   # Run from stage N to end
./run --status         # Show current status
```

The wrapper:
- Sets PYTHONUNBUFFERED=1 so output isn't lost on crashes
- Tees output to timestamped log files in output/logs/

Never run stage modules directly (e.g., `python -m pipeline.stage2_individual`).

### Running Python Code

Dependencies are specified via inline `# /// script` headers (PEP 723), so running `python` or `uv run python` directly won't have dependencies available.

To verify code compiles, use the wrapper script:
```bash
./run --help   # Imports all modules, verifies syntax
./run --status # Also runs queries against the database
```

### Code Changes

- Keep code clean and clear
- Flag significant logic changes for discussion before running code
- Update RULES.md when adding/modifying rules
- Update TODO.md when discovering new work items
- Stages can always be re-run after fixes

## Current State

Check `./run_pipeline.py --status` to see what stages have been completed and current counts.
