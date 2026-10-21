"""Smoke-test the rider-matching engine against bundled sample data.

There's no model to train here: ``app/matching`` is a deterministic rules
engine (ported from ML/Algorithm), not a statistical model, so there's no
``.fit()`` step and no artifact to persist. This script replaces the old
iris-classifier training step with a quick end-to-end check that the engine
still buckets and matches riders as expected before you deploy a change —
run it after editing anything under ``app/matching/``.
"""

import json

from app.matching import run_matching, serialize_result
from app.matching.rider import rider_lite_from_input

SAMPLE_RIDERS = [
    {
        "user_id": "u1",
        "flight_id": 1,
        "date": "2026-11-21",
        "earliest_time": "08:00:00",
        "latest_time": "09:30:00",
        "airport": "ONT",
        "to_airport": True,
        "school": "POMONA",
        "name": "Alex Rivera",
        "bags_no": 1,
    },
    {
        "user_id": "u2",
        "flight_id": 2,
        "date": "2026-11-21",
        "earliest_time": "08:30:00",
        "latest_time": "09:45:00",
        "airport": "ONT",
        "to_airport": True,
        "school": "POMONA",
        "name": "Jordan Lee",
        "bags_no": 2,
    },
    {
        "user_id": "u3",
        "flight_id": 3,
        "date": "2026-11-21",
        "earliest_time": "14:00:00",
        "latest_time": "14:30:00",
        "airport": "LAX",
        "to_airport": False,
        "school": "POMONA",
        "name": "Sam Patel",
        "bags_no": 1,
    },
]


def run_smoke_test() -> None:
    riders = [rider_lite_from_input(r) for r in SAMPLE_RIDERS]
    result = run_matching(riders)
    output = serialize_result(result)

    print(f"Buckets: {output['summary']['buckets']} | "
          f"Groups: {output['summary']['groups']} | "
          f"Unmatched: {output['summary']['unmatched_riders']}")
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    run_smoke_test()
