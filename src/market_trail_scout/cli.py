import argparse
from dotenv import load_dotenv

from services.breakout_exporter import BreakoutExporter
from services.breakout_scorer import BreakoutScorer
from services.breakout_screener import BreakoutScreener
from services.data_initializer import DataInitializer
from services.fidelity_trade_importer import FidelityTradeImporter
from services.fifo_trade_matcher import FifoTradeMatcher

def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)

    subparsers.add_parser('init', help='Initialize and load historical data')
    subparsers.add_parser('fu', help='fundamentals update')
    subparsers.add_parser('eu', help='End Of Date Update')
    subparsers.add_parser('wedge', help='perform analysis')
    subparsers.add_parser('bs', help='breakout screener')

    # ✅ Add Fidelity Trade Import (fti) subcommand
    fti_parser = subparsers.add_parser('fti', help='Import Fidelity trades')
    fti_parser.add_argument('--csv', required=True, help='Path to the Fidelity CSV file')

    fti_parser = subparsers.add_parser('ftm', help='Fifo Trade Matcher')

    args = parser.parse_args()

    if args.command == 'init':
        data_initalizer = DataInitializer()
        data_initalizer.update_symbols_list()
        data_initalizer.initialize_data()

    elif args.command == 'fu':
        data_initalizer = DataInitializer()
        data_initalizer.update_recent_fundamentals()

    elif args.command == 'eu':
        data_initalizer = DataInitializer()
        data_initalizer.update()

    elif args.command == 'wedge':
        screener = BreakoutScreener()
        df, history_df = screener.swing_slope_breakout(
            resistance_r2=0.2,
            support_r2=0.2,
            pivot_count=3,
            require_positive_support=True,
            require_flat_or_dropping_resistance=True,
            get_full_history=True
        )

        if not df.empty:
            exporter = BreakoutExporter()
            excel_path = exporter.export_to_excel(df)

            if history_df is not None:
                print("📊 Generating breakout charts from full history...")
                exporter.export_charts(df, history_df, excel_path)

            print(df.head(10))
        else:
            print("⚠️ No breakout candidates found.")

    elif args.command == 'bs':
        screener = BreakoutScreener()
        phase1_df = screener.sound_base_breakout()
        if not phase1_df.empty:
            exporter = BreakoutExporter(filename_base='breakout_screener_raw')
            excel_path = exporter.export_to_excel(phase1_df)

        symbols = phase1_df["symbol"].tolist()
        scorer = BreakoutScorer()
        scores = scorer.evaluate_candidates(symbols)

        if scores:
            df_scores = scorer.to_dataframe(scores)
            exporter = BreakoutExporter(filename_base='breakout_screener')
            excel_path = exporter.export_to_excel(df_scores)
            print(df_scores.head(10))
        else:
            print("⚠️ No candidates passed Phase 2 scoring.")

    elif args.command == 'fti':
        importer = FidelityTradeImporter(args.csv)
        importer.import_trades()

    elif args.command == 'ftm':
        matcher = FifoTradeMatcher()
        result_df = matcher.run()

        if not result_df.empty:
            print(f"✅ Matched {len(result_df)} FIFO trades")
            exporter = BreakoutExporter(filename_base='fifo_trade_matches')
            exporter.export_to_excel(result_df)
        else:
            print("⚠️ No closed FIFO trade matches found.")


if __name__ == "__main__":
    main()
