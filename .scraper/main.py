import argparse
import logging
import shutil
import sys
from pathlib import Path

import no_intro
import redump


def main() -> None:
    logging.basicConfig(
        format="[%(asctime)s %(levelname)s %(name)s] %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG,
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("output_dir")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    redump_dir = output_dir / "redump"
    if args.clean and redump_dir.exists():
        shutil.rmtree(redump_dir)
    redump.scrape(redump_dir)

    no_intro_dir = output_dir / "no-intro"
    if args.clean and no_intro_dir.exists():
        shutil.rmtree(no_intro_dir)
    no_intro.scrape(no_intro_dir)


if __name__ == "__main__":
    sys.exit(main())
