"""
Stage 2: Individual Classification

Apply rules based on photo's own properties (path, size, dimensions, filename).
Rules are functions that examine one photo in isolation.

Two outcomes:
- reject: Junk - discard this photo
- separate: Keep but handle differently (skips expensive later stages)

Output: `individual_decisions` table (photo_id, decision, rule_name)
"""

from collections import defaultdict

from tqdm import tqdm

from .database import (
    get_connection,
    get_photos_without_decision,
    record_stage_completion,
)
from .rules.individual import apply_individual_rules


def run_stage2(clear_existing: bool = False) -> None:
    """
    Run Stage 2: Individual Classification.

    Args:
        clear_existing: If True, clear existing decisions before running
    """
    print("=" * 70)
    print("STAGE 2: INDIVIDUAL CLASSIFICATION")
    print("=" * 70)
    print()

    with get_connection() as conn:
        if clear_existing:
            print("Clearing existing Stage 2 data...")
            conn.execute("DELETE FROM individual_decisions")
            conn.commit()

        # Get photos that need classification
        print("Loading photos for classification...")
        photos = get_photos_without_decision(conn)
        print(f"Found {len(photos):,} photos to classify")
        print()

        if not photos:
            print("No photos to classify.")
            return

        # Track stats
        stats = defaultdict(int)
        decisions = []

        # Apply rules to each photo
        for photo in tqdm(photos, desc="Classifying photos"):
            result = apply_individual_rules(photo)
            if result:
                decision, rule_name = result
                decisions.append({
                    "photo_id": photo["id"],
                    "decision": decision,
                    "rule_name": rule_name,
                })
                stats[f"{decision}:{rule_name}"] += 1

        # Insert decisions
        if decisions:
            conn.executemany(
                """
                INSERT INTO individual_decisions (photo_id, decision, rule_name)
                VALUES (:photo_id, :decision, :rule_name)
                """,
                decisions,
            )
            conn.commit()

        # Calculate summary stats
        total_rejected = sum(v for k, v in stats.items() if k.startswith("reject:"))
        total_separated = sum(v for k, v in stats.items() if k.startswith("separate:"))

        # Record completion
        record_stage_completion(
            conn, "2",
            len(decisions),
            f"rejected={total_rejected}, separated={total_separated}"
        )

    # Print summary
    print()
    print("=" * 70)
    print("STAGE 2 COMPLETE")
    print("=" * 70)
    print()
    print(f"Photos classified:    {len(photos):,}")
    print(f"  Rejected:           {total_rejected:,}")
    print(f"  Separated:          {total_separated:,}")
    print(f"  Passed through:     {len(photos) - len(decisions):,}")
    print()

    if stats:
        print("Breakdown by rule:")
        # Group by decision type
        rejection_stats = {k: v for k, v in stats.items() if k.startswith("reject:")}
        separation_stats = {k: v for k, v in stats.items() if k.startswith("separate:")}

        if rejection_stats:
            print("\n  Rejections:")
            for key, count in sorted(rejection_stats.items(), key=lambda x: -x[1]):
                rule = key.split(":")[1]
                print(f"    {rule}: {count:,}")

        if separation_stats:
            print("\n  Separations:")
            for key, count in sorted(separation_stats.items(), key=lambda x: -x[1]):
                rule = key.split(":")[1]
                print(f"    {rule}: {count:,}")

    print()


if __name__ == "__main__":
    run_stage2()
