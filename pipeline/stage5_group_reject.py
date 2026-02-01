"""
Stage 5: Group Rejection

Apply rules based on relationship to other group members.
Rules can use hamming distance between specific photos as evidence.

Output: `group_rejections` table (photo_id, group_id, rule_name)
"""

from collections import defaultdict

from tqdm import tqdm

from .database import (
    get_connection,
    get_all_group_ids,
    get_group_members,
    record_stage_completion,
)
from .rules.group import apply_group_rules


def run_stage5(clear_existing: bool = False) -> None:
    """
    Run Stage 5: Group Rejection.

    Args:
        clear_existing: If True, clear existing rejections before running
    """
    print("=" * 70)
    print("STAGE 5: GROUP REJECTION")
    print("=" * 70)
    print()

    with get_connection() as conn:
        if clear_existing:
            print("Clearing existing Stage 5 data...")
            conn.execute("DELETE FROM group_rejections")
            conn.commit()

        # Get all group IDs
        print("Loading duplicate groups...")
        group_ids = get_all_group_ids(conn)
        print(f"Found {len(group_ids):,} duplicate groups")
        print()

        if not group_ids:
            print("No groups to process.")
            record_stage_completion(conn, "5", 0, "no groups")
            return

        # Track stats
        stats = defaultdict(int)
        total_rejections = 0

        rejection_batch = []

        def flush_batch():
            nonlocal rejection_batch
            if rejection_batch:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO group_rejections (photo_id, group_id, rule_name)
                    VALUES (:photo_id, :group_id, :rule_name)
                    """,
                    rejection_batch,
                )
                rejection_batch = []
            conn.commit()

        # Process each group
        for group_id in tqdm(group_ids, desc="Processing groups"):
            # Get group members with full data
            members = get_group_members(conn, group_id)

            if len(members) < 2:
                continue

            # Apply group rules
            rejections = apply_group_rules(members)

            for rejected_id, rule_name in rejections:
                rejection_batch.append({
                    "photo_id": rejected_id,
                    "group_id": group_id,
                    "rule_name": rule_name,
                })
                stats[rule_name] += 1
                total_rejections += 1

            # Flush periodically
            if len(rejection_batch) >= 1000:
                flush_batch()

        # Final flush
        flush_batch()

        # Record completion
        record_stage_completion(conn, "5", total_rejections)

    # Print summary
    print()
    print("=" * 70)
    print("STAGE 5 COMPLETE")
    print("=" * 70)
    print()
    print(f"Groups processed:       {len(group_ids):,}")
    print(f"Photos rejected:        {total_rejections:,}")
    print()

    if stats:
        print("Rejections by rule:")
        for rule_name, count in sorted(stats.items(), key=lambda x: -x[1]):
            print(f"  {rule_name}: {count:,}")

    print()


if __name__ == "__main__":
    run_stage5()
