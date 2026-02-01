# Claude Instructions for Photo Recovery Pipeline

## Required Reading

Before doing any work on this project, read these files:
1. **PLAN.md** - Architecture and design decisions (MUST follow)
2. **TODO.md** - Current work items and pending tasks
3. **RULES.md** - Documentation of all classification rules

## Key Operational Requirements

### Running Pipeline Stages

Always use the main entry point:
```bash
./run_pipeline.py --stage N        # Run single stage
./run_pipeline.py --from-stage N   # Run from stage N to end
./run_pipeline.py --status         # Show current status
```

Never run stage modules directly (e.g., `python -m pipeline.stage2_individual`).

### Logging

All pipeline runs should log to both stdout AND a file for shared visibility:
```bash
./run_pipeline.py --stage 2 2>&1 | tee output/logs/stage2_$(date +%Y%m%d_%H%M%S).log
```

Create the logs directory if needed: `mkdir -p output/logs`

### Code Changes

- Keep code clean and clear
- Flag significant logic changes for discussion before running code
- Update RULES.md when adding/modifying rules
- Update TODO.md when discovering new work items
- Stages can always be re-run after fixes

## Current State

Check `./run_pipeline.py --status` to see what stages have been completed and current counts.
