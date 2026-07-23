import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.utils.config_loader import load_and_resolve_config
from src.extractors.generic_api import GenericAPIExtractor

import argparse
import sys
import os
from dotenv import load_dotenv

from src.utils.config_loader import load_and_resolve_config
from src.extractors.generic_api import GenericAPIExtractor

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Metadata-Driven API Ingestion CLI")
    
    parser.add_argument(
        "--api", 
        type=str, 
        required=True,
        help="Target API to ingest (must match a JSON filename in config/)"
    )
    
    parser.add_argument(
        "--sink",
        type=str,
        choices=["bq", "local"],
        default="bq",
        help="Destination for the extracted data (default: bq)"
    )

    args = parser.parse_args()

    try:
        config_path = os.path.join("config", f"{args.api}.json")
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        config = load_and_resolve_config(config_path)

        # Pass the sink option to the extractor
        extractor = GenericAPIExtractor(config=config, sink=args.sink)
        extractor.run()

    except Exception as e:
        print(f"Error during ingestion: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()