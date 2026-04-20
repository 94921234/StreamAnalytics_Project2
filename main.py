"""
main.py — CLI entry point for the synthetic data generator.

Usage:
    python main.py # default configuration
    python main.py --config config.json # from JSON file
    python main.py --num-orders 2000 --sim-hours 24 # CLI overrides
    python main.py --sim-start "2025-01-11 10:00:00" # Saturday (weekend)

All CLI flags override the values ​​in the config file (if `--config` is used), 
which in turn override the defaults in the data class.
"""

import argparse

from config import Config, load_config_from_file
from generator import DataGenerator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="🍕 Food Delivery Synthetic Data Generator — Madrid · Stream Analytics M1",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py
  python main.py --config config.json
  python main.py --num-orders 2000 --sim-hours 24 --output-dir ./data
  python main.py --sim-start "2025-01-11 10:00:00"   # saturday (weekend)
  python main.py --num-restaurants 30 --num-couriers 80 --num-orders 1000
        """,
    )
    parser.add_argument(
        "--config", type=str, default=None, metavar="PATH",
        help="Path to the JSON configuration file",
    )
    parser.add_argument(
        "--num-orders", type=int, default=None,
        help="Total number of orders to generate",
    )
    parser.add_argument(
        "--num-restaurants", type=int, default=None,
        help="Number of restaurants in the simulation",
    )
    parser.add_argument(
        "--num-couriers", type=int, default=None,
        help="Number of couriers in the fleet",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None, metavar="DIR",
        help="Output directory for generated files",
    )
    parser.add_argument(
        "--sim-start", type=str, default=None, metavar="DATETIME",
        help='Start of simulation in ISO format (ej. "2025-01-13 08:00:00")',
    )
    parser.add_argument(
        "--sim-hours", type=int, default=None,
        help="Number of hours to simulate",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    # ── Load config base ────────────────────────────────────────────────
    if args.config:
        config = load_config_from_file(args.config)
        print(f"[Config] Loaded from '{args.config}'")
    else:
        config = Config()
        print("[Config] Using default settings")

    # ── Overrides de CLI ──────────────────────────────
    if args.num_orders      is not None: config.num_orders      = args.num_orders
    if args.num_restaurants is not None: config.num_restaurants = args.num_restaurants
    if args.num_couriers    is not None: config.num_couriers    = args.num_couriers
    if args.output_dir      is not None: config.output_dir      = args.output_dir
    if args.sim_start       is not None: config.simulation_start = args.sim_start
    if args.sim_hours       is not None: config.simulation_hours = args.sim_hours

    # ── Execute ──────────────────────────────────────────────────────────
    generator = DataGenerator(config)
    generator.run()


if __name__ == "__main__":
    main()
