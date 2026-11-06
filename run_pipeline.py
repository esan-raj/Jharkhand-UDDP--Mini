import argparse
import json
import logging
import time

from uddp.pipeline import run_pipeline, window_batch_id

logger = logging.getLogger("uddp")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the Jharkhand Mini-UDDP pipeline.")
    parser.add_argument("--batch-id", help="Reuse a batch id. Required with --from-bronze.")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Load the CSV files already stored in data/raw.",
    )
    parser.add_argument(
        "--from-bronze",
        action="store_true",
        help="Replay silver and candidate gold from bronze for --batch-id. Published gold stays as it is.",
    )
    parser.add_argument(
        "--promote",
        action="store_true",
        help="Publish candidate gold after the quality gate. Required to replace the snapshot during replay.",
    )
    parser.add_argument(
        "--schedule-seconds",
        type=int,
        default=0,
        help="Run forever, sleeping this many seconds between successful or failed runs.",
    )
    args = parser.parse_args(argv)

    def once(batch_id):
        summary = run_pipeline(
            batch_id=batch_id,
            skip_download=args.skip_download,
            from_bronze=args.from_bronze,
            promote=True if args.promote else None,
        )
        print(json.dumps(summary, indent=2))
        return summary

    if args.schedule_seconds <= 0:
        once(args.batch_id)
        return

    while True:
        batch_id = args.batch_id or window_batch_id(args.schedule_seconds)
        try:
            once(batch_id)
        except Exception:
            logger.exception("Scheduled run failed")
        time.sleep(args.schedule_seconds)


if __name__ == "__main__":
    main()
