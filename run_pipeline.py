#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "python-magic",
#   "pillow",
#   "imagehash",
#   "piexif",
#   "tqdm"
# ]
# ///
"""
Photo Recovery Pipeline - Main Entry Point

Run the full pipeline or individual stages for extracting family photos
from archived computer contents.

Usage:
    # Full run from scratch
    ./run_pipeline.py --from-stage 1

    # Re-run from Stage 4 onwards (after fixing grouping logic)
    ./run_pipeline.py --from-stage 4

    # Run single stage
    ./run_pipeline.py --stage 2

    # Import perceptual hashes from old database
    ./run_pipeline.py --stage 3 --import-hashes ../old/photos.db

    # Show pipeline status
    ./run_pipeline.py --status
"""

import argparse
import sys
from pathlib import Path

from pipeline.config import DB_PATH, SOURCE_ROOT, EXPORT_DIR, OLD_DB_PATH, FILES_DIR
from pipeline.database import get_connection, init_db


# Stage order for --from-stage
STAGE_ORDER = ["1", "1b", "2", "3", "4", "5", "6"]


def show_status():
    """Show current pipeline status."""
    print("=" * 70)
    print("PIPELINE STATUS")
    print("=" * 70)
    print()

    if not DB_PATH.exists():
        print("Database not found. Run Stage 1 to initialize.")
        return

    # Initialize schema if needed (adds new tables)
    init_db()

    with get_connection() as conn:
        # Check if this is the old schema (no pipeline_state table)
        cursor = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='pipeline_state'
        """)
        has_new_schema = cursor.fetchone() is not None

        if not has_new_schema:
            print("Old database schema detected.")
            print("Run './run_pipeline.py --from-stage 1' to migrate to new pipeline.")
            print()
            # Show basic stats from old schema
            total = conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0]
            print(f"Total photos (old schema): {total:,}")
            return

        # Stage completion status
        cursor = conn.execute("""
            SELECT stage, completed_at, photo_count, notes
            FROM pipeline_state
            ORDER BY stage
        """)
        stages = list(cursor.fetchall())

        if stages:
            print("Completed stages:")
            for row in stages:
                print(f"  Stage {row['stage']}: {row['completed_at']}")
                print(f"           Count: {row['photo_count']:,}")
                if row['notes']:
                    print(f"           Notes: {row['notes']}")
            print()

        # Photo counts
        total = conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0]
        print(f"Total photos:          {total:,}")

        paths = conn.execute("SELECT COUNT(*) FROM photo_paths").fetchone()[0]
        print(f"Total paths:           {paths:,}")

        decisions = conn.execute("SELECT COUNT(*) FROM individual_decisions").fetchone()[0]
        print(f"Individual decisions:  {decisions:,}")

        with_hash = conn.execute(
            "SELECT COUNT(*) FROM photos WHERE perceptual_hash IS NOT NULL"
        ).fetchone()[0]
        print(f"With perceptual hash:  {with_hash:,}")

        in_groups = conn.execute("SELECT COUNT(*) FROM duplicate_groups").fetchone()[0]
        print(f"In duplicate groups:   {in_groups:,}")

        group_count = conn.execute(
            "SELECT COUNT(DISTINCT group_id) FROM duplicate_groups"
        ).fetchone()[0]
        print(f"Duplicate groups:      {group_count:,}")

        rejected = conn.execute("SELECT COUNT(*) FROM group_rejections").fetchone()[0]
        print(f"Group rejections:      {rejected:,}")

        # Decision breakdown
        print()
        print("Individual decision breakdown:")
        cursor = conn.execute("""
            SELECT decision, rule_name, COUNT(*) as count
            FROM individual_decisions
            GROUP BY decision, rule_name
            ORDER BY count DESC
        """)
        for row in cursor:
            print(f"  {row['decision']}/{row['rule_name']}: {row['count']:,}")

        # Group rejection breakdown
        print()
        print("Group rejection breakdown:")
        cursor = conn.execute("""
            SELECT rule_name, COUNT(*) as count
            FROM group_rejections
            GROUP BY rule_name
            ORDER BY count DESC
        """)
        for row in cursor:
            print(f"  {row['rule_name']}: {row['count']:,}")

    print()


def run_stage(stage: str, args: argparse.Namespace):
    """Run a specific stage."""
    if stage == "1":
        from pipeline.stage1_scan import run_stage1
        run_stage1(
            source_root=Path(args.source) if args.source else SOURCE_ROOT,
            clear_existing=args.clear
        )

    elif stage == "1b":
        from pipeline.stage1b_link import run_stage1b
        run_stage1b(
            files_dir=FILES_DIR,
            clear_existing=args.clear
        )

    elif stage == "2":
        from pipeline.stage2_individual import run_stage2
        run_stage2(clear_existing=args.clear)

    elif stage == "3":
        from pipeline.stage3_phash import run_stage3
        import_from = Path(args.import_hashes) if args.import_hashes else None
        run_stage3(import_from=import_from, clear_existing=args.clear)

    elif stage == "4":
        from pipeline.stage4_group import run_stage4
        run_stage4(clear_existing=args.clear)

    elif stage == "5":
        from pipeline.stage5_group_reject import run_stage5
        run_stage5(clear_existing=args.clear)

    elif stage == "6":
        from pipeline.stage6_export import run_stage6
        run_stage6(
            export_dir=Path(args.export_dir) if args.export_dir else EXPORT_DIR,
            use_hardlinks=not args.copy,
            clear_existing=args.clear
        )

    else:
        print(f"Unknown stage: {stage}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Photo Recovery Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Stage selection (mutually exclusive)
    stage_group = parser.add_mutually_exclusive_group()
    stage_group.add_argument(
        "--stage", type=str, choices=STAGE_ORDER,
        help="Run a single stage"
    )
    stage_group.add_argument(
        "--from-stage", type=str, choices=STAGE_ORDER,
        help="Run from this stage to the end"
    )
    stage_group.add_argument(
        "--status", action="store_true",
        help="Show pipeline status"
    )

    # Stage-specific options
    parser.add_argument(
        "--source", type=str,
        help="Source directory for Stage 1 (default: from config)"
    )
    parser.add_argument(
        "--import-hashes", type=str,
        help="Import perceptual hashes from old database (Stage 3)"
    )
    parser.add_argument(
        "--export-dir", type=str,
        help="Export directory for Stage 6 (default: from config)"
    )
    parser.add_argument(
        "--copy", action="store_true",
        help="Use file copies instead of hardlinks (Stage 6)"
    )

    # General options
    parser.add_argument(
        "--clear", action="store_true",
        help="Clear existing data before running stage"
    )

    args = parser.parse_args()

    # Show status
    if args.status:
        show_status()
        return

    # Validate we have a stage to run
    if args.stage is None and args.from_stage is None:
        parser.print_help()
        print("\nError: Specify --stage, --from-stage, or --status")
        sys.exit(1)

    # Initialize database
    init_db()

    # Run stage(s)
    if args.stage:
        run_stage(args.stage, args)
    else:
        # Run from specified stage to end
        start_idx = STAGE_ORDER.index(args.from_stage)
        for stage in STAGE_ORDER[start_idx:]:
            run_stage(stage, args)


if __name__ == "__main__":
    main()
