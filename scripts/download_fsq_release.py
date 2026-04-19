import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from huggingface_hub import snapshot_download


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True, help="Release date, e.g. 2026-04-14")
    parser.add_argument(
        "--out",
        default=str(Path("data") / "raw"),
        help="Output folder for raw dataset shards",
    )
    args = parser.parse_args()

    token = os.getenv("HF_TOKEN")
    if not token:
        raise SystemExit(
            "HF_TOKEN is missing. Create a .env with HF_TOKEN=... (Hugging Face access token)."
        )

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Pull just the parquet shards we need for this release date.
    allow_patterns = [
        f"release/dt={args.dt}/places/parquet/*",
        f"release/dt={args.dt}/places/parquet/**/*",
        f"release/dt={args.dt}/categories/parquet/*",
        f"release/dt={args.dt}/categories/parquet/**/*",
    ]

    print(f"Downloading release dt={args.dt} parquet shards to {out_dir}")
    snapshot_download(
        repo_id="foursquare/fsq-os-places",
        repo_type="dataset",
        local_dir=str(out_dir),
        allow_patterns=allow_patterns,
        token=token,
    )
    print("Done.")


if __name__ == "__main__":
    main()

