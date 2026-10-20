"""Offline command-line validation of synthetic JSON event batches."""
import argparse
import json
import math
from pathlib import Path
import sys

from src.streaming_pipeline import process_event_batch


def reject_non_finite(value):
    """Python accepts NaN/Infinity by default, but they are not JSON numbers."""
    raise ValueError("Non-finite JSON number is not supported: " + value)


def parse_finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        reject_non_finite(value)
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="UTF-8 JSON array of events")
    args = parser.parse_args(argv)

    try:
        with args.input.open(encoding="utf-8") as source:
            events = json.load(
                source, parse_constant=reject_non_finite,
                parse_float=parse_finite_float,
            )
    except (OSError, UnicodeError, ValueError) as error:
        print("Unable to read event batch: " + str(error), file=sys.stderr)
        return 2

    if not isinstance(events, list):
        print("Event batch must be a JSON array", file=sys.stderr)
        return 2

    valid, quarantined = process_event_batch(events)
    report = {
        "valid_events": valid,
        "quarantined": [
            {"event": event, "errors": errors}
            for event, errors in quarantined
        ],
    }
    json.dump(report, sys.stdout, indent=2, allow_nan=False)
    print()
    return 1 if quarantined else 0


if __name__ == "__main__":
    sys.exit(main())
