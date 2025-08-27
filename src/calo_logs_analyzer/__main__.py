import argparse

from src.calo_logs_analyzer.report import run_analysis


def main():
    parser = argparse.ArgumentParser(prog="calo-logs-analyzer")
    parser.add_argument("--log_dir", help="Folder containing log files")
    parser.add_argument("--out", default="./out", help="Output folder (default: ./out)")
    parser.add_argument("--raw-csv", action="store_true", help="Also export raw parsed CSV")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Logging level (default: INFO)")
    parser.add_argument("--tolerance", type=float, default=0.005,
                        help="Tolerance when comparing balances (default: 0.005)")
    parser.add_argument("--decimals", type=int, default=2,
                        help="Default decimal places for rounding when currency is unknown (default: 2)")
    parser.add_argument("--excel-columns", default="accounting",
                        help="Columns preset for the Excel ledger sheet: 'accounting', 'full' or path to a JSON file")
    args = parser.parse_args()

    run_analysis(
        args.log_dir,
        args.out,
        export_raw=args.raw_csv,
        log_level=args.log_level,
        tolerance=args.tolerance,
        decimals=args.decimals,
        excel_columns=args.excel_columns
    )


if __name__ == "__main__":
    main()
