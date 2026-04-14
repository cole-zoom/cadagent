"""Interactive CLI for reviewing pending header mapping candidates.

Usage:
    python scripts/review_mappings.py [--department fin]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.clients.bigquery import BigQueryClient
from shared.config.logging import configure_logging
from shared.config.settings import settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Review pending header mapping candidates")
    parser.add_argument("--department", help="Filter by department")
    parser.add_argument("--limit", type=int, default=20, help="Number of candidates to review")
    args = parser.parse_args()

    configure_logging(settings.log_level, service="review")
    bq = BigQueryClient(project_id=settings.gcp_project_id)
    project = settings.gcp_project_id
    stg = settings.bq_stg_dataset

    where = "WHERE hmc.approved_flag IS NULL AND hmc.candidate_score < 0.8"
    if args.department:
        where += f" AND h.department_id = '{args.department}'"

    sql = f"""
        SELECT
            hmc.header_id,
            h.header_raw,
            h.header_normalized,
            h.department_id,
            hmc.canonical_entity_type,
            hmc.canonical_entity_id,
            hmc.candidate_method,
            hmc.candidate_score
        FROM `{project}.{stg}.header_mapping_candidates` hmc
        JOIN `{project}.{stg}.headers` h ON hmc.header_id = h.header_id
        {where}
        ORDER BY hmc.candidate_score DESC
        LIMIT {args.limit}
    """

    candidates = bq.query(sql)

    if not candidates:
        print("No pending candidates to review.")
        return

    print(f"\n{'='*80}")
    print(f"Pending mapping candidates ({len(candidates)} found)")
    print(f"{'='*80}\n")

    for i, c in enumerate(candidates, 1):
        print(f"[{i}/{len(candidates)}]")
        print(f"  Header raw:      {c['header_raw']}")
        print(f"  Normalized:      {c['header_normalized']}")
        print(f"  Department:      {c['department_id']}")
        print(f"  Entity type:     {c['canonical_entity_type']}")
        print(f"  Candidate ID:    {c['canonical_entity_id']}")
        print(f"  Method:          {c['candidate_method']}")
        print(f"  Score:           {c['candidate_score']:.2f}")
        print()

        response = input("  [a]pprove / [r]eject / [s]kip / [q]uit: ").strip().lower()

        if response == "q":
            break
        elif response == "a":
            _update_candidate(bq, project, stg, c["header_id"], approved=True)
            print("  -> Approved")
        elif response == "r":
            _update_candidate(bq, project, stg, c["header_id"], approved=False)
            print("  -> Rejected")
        else:
            print("  -> Skipped")

        print()


def _update_candidate(bq: BigQueryClient, project: str, stg: str, header_id: str, approved: bool) -> None:
    sql = f"""
        UPDATE `{project}.{stg}.header_mapping_candidates`
        SET approved_flag = {str(approved).upper()},
            approved_by = 'manual_review',
            approved_at = CURRENT_TIMESTAMP()
        WHERE header_id = '{header_id}'
    """
    bq.execute_ddl(sql)


if __name__ == "__main__":
    main()
