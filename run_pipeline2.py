#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "pillow",
#   "imagehash",
#   "tqdm"
# ]
# ///
"""
Pipeline 2: Post-Curation Processing

Works on the curated output of pipeline1 (after manual review).

Stages:
  1. Rehash - compute extended hashes (phash_16, colorhash) for kept photos
  1b. Pairs - compute all pairwise distances (for threshold tuning)
  2. Regroup - two-stage clustering with tuned pHash16+colorHash thresholds
  3. Composite - join primary and secondary groups into composite groups

Usage:
    ./run_pipeline2.py --stage 1     # Compute extended hashes
    ./run_pipeline2.py --stage 1b    # Compute all pairwise distances
    ./run_pipeline2.py --stage 2     # Regroup with tuned thresholds
    ./run_pipeline2.py --stage 3     # Create composite groups
    ./run_pipeline2.py --status      # Show pipeline2 status
"""

import argparse
import sys

from pipeline.config import DB_PATH
from pipeline.database import get_connection, init_db
from pipeline2.config import STAGE_ORDER


def show_status():
    """Show current pipeline2 status."""
    print("=" * 70)
    print("PIPELINE2 STATUS")
    print("=" * 70)
    print()

    if not DB_PATH.exists():
        print("Database not found.")
        return

    with get_connection() as conn:
        # Pipeline2 stage completion
        cursor = conn.execute("""
            SELECT stage, completed_at, photo_count, notes
            FROM pipeline_state
            WHERE stage LIKE 'p2_%'
            ORDER BY stage
        """)
        stages = list(cursor.fetchall())

        if stages:
            print("Completed stages:")
            for row in stages:
                stage_num = row['stage'].replace('p2_', '')
                print(f"  Stage {stage_num}: {row['completed_at']}")
                print(f"           Count: {row['photo_count']:,}")
                if row['notes']:
                    print(f"           Notes: {row['notes']}")
            print()

        # Extended hashes stats
        cursor = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='extended_hashes'
        """)
        has_extended = cursor.fetchone() is not None

        if has_extended:
            eh_count = conn.execute(
                "SELECT COUNT(*) FROM extended_hashes"
            ).fetchone()[0]
            print(f"Extended hashes:       {eh_count:,}")
        else:
            print("Extended hashes not yet computed.")

        # p2_groups stats
        cursor = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='p2_groups'
        """)
        has_p2_groups = cursor.fetchone() is not None

        if has_p2_groups:
            group_count = conn.execute(
                "SELECT COUNT(DISTINCT group_id) FROM p2_groups"
            ).fetchone()[0]
            photo_count = conn.execute(
                "SELECT COUNT(*) FROM p2_groups"
            ).fetchone()[0]
            print(f"P2 groups:             {group_count:,}")
            print(f"Photos in P2 groups:   {photo_count:,}")

            # Check for unlinked pairs table
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='p2_unlinked_pairs'
            """)
            if cursor.fetchone():
                unlinked = conn.execute(
                    "SELECT COUNT(*) FROM p2_unlinked_pairs"
                ).fetchone()[0]
                print(f"Unlinked pairs:        {unlinked:,}")
        else:
            print("Regrouping not yet run.")

        # composite_groups stats
        cursor = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='composite_groups'
        """)
        has_composite = cursor.fetchone() is not None

        if has_composite:
            group_count = conn.execute(
                "SELECT COUNT(DISTINCT group_id) FROM composite_groups"
            ).fetchone()[0]
            photo_count = conn.execute(
                "SELECT COUNT(*) FROM composite_groups"
            ).fetchone()[0]
            print(f"Composite groups:      {group_count:,}")
            print(f"Photos in composite:   {photo_count:,}")

    print()


def run_stage(stage: str, args: argparse.Namespace):
    """Run a specific stage."""
    if stage == "1":
        from pipeline2.stage1_rehash import run_stage1
        run_stage1(clear_existing=args.clear)

    elif stage == "1b":
        from pipeline2.stage1b_pairs import run_stage1b
        run_stage1b(clear_existing=args.clear)

    elif stage == "2":
        from pipeline2.stage2_regroup import run_stage2
        run_stage2()

    elif stage == "3":
        from pipeline2.stage3_composite import run_stage3
        run_stage3()

    else:
        print(f"Unknown stage: {stage}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline 2: Post-Curation Processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Stage selection
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
        help="Show pipeline2 status"
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

    # Initialize database (ensures schema exists)
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
