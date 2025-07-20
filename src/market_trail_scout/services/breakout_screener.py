import os
import traceback
from pathlib import Path

import duckdb
import pandas as pd
import sqlparse
from appdirs import user_data_dir


class BreakoutScreener:
    def __init__(self, db_path: str = None, sma_days: int = 50, base_days: int = 60, max_range_pct: float = 5.0):
        try:
            self.sma_days = sma_days
            self.base_days = base_days
            self.max_range_pct = max_range_pct

            # Use default data location if not provided
            if db_path:
                self.db_path = Path(db_path)
            else:
                raw_dir = os.environ.get('SCREENER_ENGINE_DATA_DIR', '')
                expanded_dir = os.path.expandvars(raw_dir)
                data_dir = Path(expanded_dir) if expanded_dir else Path(user_data_dir('ScreenerEngine', 'Miltonstreet'))
                data_dir.mkdir(parents=True, exist_ok=True)
                self.db_path = data_dir / 'stock_data.duckdb'

            self.db = duckdb.connect(str(self.db_path))
            self._V_SWING_SLOPE_BREAKOUT = "v_swing_slope_breakout"
            self._create_or_replace_default_view("sql/swing_slope_breakout.sql", "v_swing_slope_breakout")
            self._create_or_replace_default_view("sql/rising_stock_finder.sql", "v_rising_stock_finder")
            self._create_or_replace_default_view("sql/sound_breakout_candidates.sql", "v_sound_breakout_candidates")

        except Exception as e:
            print("❌ Error initializing BreakoutScreener:")
            traceback.print_exc()
            raise

    def _create_or_replace_default_view(self, sql_file: str, view_name: str):
        """
        Automatically drop and recreate the specified view from a SQL file on init.
        """
        try:
            query_path = Path(__file__).parent / sql_file
            if not query_path.exists():
                print(f"❌ SQL file not found: {query_path}")
                return

            raw_sql = query_path.read_text()
            #print(f"🔁 Refreshing view '{view_name}' from: {query_path}")

            self.db.execute(f"DROP VIEW IF EXISTS {view_name}")
            self.db.execute(f"CREATE VIEW {view_name} AS {raw_sql}")
            #print(f"✅ View '{view_name}' created successfully.")

        except Exception as e:
            print(f"\n❌ Failed to refresh view '{view_name}' from '{sql_file}'")
            print("💥 Exception message:")
            print(f"👉 {str(e)}")
            print("🧵 Full traceback:")
            traceback.print_exc()

    def sound_base_breakout(
            self,
            max_pct_from_high: float | None = 0.03,
            max_range_pct: float | None = 0.03,
            max_avg_move_pct: float | None = 0.02,
            min_volume_ratio: float | None = 0.5,
            max_volume_ratio: float | None = 2.5
    ) -> pd.DataFrame:
        """
        Filters sound base candidates from v_sound_breakout_candidates using customizable parameters.
        Each filter is optional — if None, it's ignored.
        """
        try:
            filters = []

            if max_pct_from_high is not None:
                filters.append(f"pct_from_20d_high >= {-max_pct_from_high}")
            if max_range_pct is not None:
                filters.append(f"pct_range_5d <= {max_range_pct}")
            if max_avg_move_pct is not None:
                filters.append(f"avg_move_pct <= {max_avg_move_pct}")
            if min_volume_ratio is not None and max_volume_ratio is not None:
                filters.append(f"volume_ratio BETWEEN {min_volume_ratio} AND {max_volume_ratio}")
            elif min_volume_ratio is not None:
                filters.append(f"volume_ratio >= {min_volume_ratio}")
            elif max_volume_ratio is not None:
                filters.append(f"volume_ratio <= {max_volume_ratio}")

            where_clause = " AND ".join(filters) if filters else "1 = 1"

            query = f"""
            SELECT
                symbol,
                security_name,
                sector,
                industry,
                last_close,
                sma_20,
                pct_from_20d_high,
                pct_range_5d,
                avg_move_pct,
                volume_ratio
            FROM v_sound_breakout_candidates
            WHERE {where_clause}
            ORDER BY pct_range_5d ASC, avg_move_pct ASC
            """

            print("📄 Executing sound base breakout query:")
            print(sqlparse.format(query, reindent=True))
            df = self.db.execute(query).df()
            print(f"✅ Found {len(df)} candidates from sound breakout base.")
            df.columns = [col.lower() for col in df.columns]
            return df

        except Exception as e:
            print("❌ Failed to query sound breakout candidates:")
            traceback.print_exc()
            return pd.DataFrame()

    def swing_slope_breakout(
            self,
            resistance_r2: float = 0.5,
            support_r2: float = 0.5,
            pivot_count: int = 3,
            require_positive_support: bool = True,
            require_flat_or_dropping_resistance: bool = True,
            get_full_history: bool = False
    ) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        """
        Returns filtered breakout candidates with optional historical OHLCV data.

        If get_full_history=True, also returns historical rows from 'eod_prices'
        between start_date and end_date for each symbol in the view result.
        """
        try:
            filters = []

            subfilters = []
            if resistance_r2 is not None:
                subfilters.append(f"resistance_r2 >= {resistance_r2}")
            if support_r2 is not None:
                subfilters.append(f"support_r2 >= {support_r2}")
            if subfilters:
                filters.append(f"({' OR '.join(subfilters)})")

            if pivot_count is not None:
                filters.append(f"pivot_high_count >= {pivot_count}")
                filters.append(f"pivot_low_count >= {pivot_count}")

            if require_positive_support:
                filters.append("support_slope >= 0")
            if require_flat_or_dropping_resistance:
                filters.append("resistance_slope <= 0")

            where_clause = " AND ".join(filters)

            base_query = f"""
            SELECT *
            FROM v_swing_slope_breakout
            WHERE {where_clause}
            ORDER BY
                sector,
                industry,
                GREATEST(resistance_r2, support_r2) DESC,
                volume_ratio DESC,
                resistance_slope ASC,
                support_slope DESC
            """

            print("📄 Executing filtered breakout query:")
            print(sqlparse.format(base_query, reindent=True))

            filtered_df = self.db.execute(base_query).df()

            if not get_full_history or filtered_df.empty:
                return filtered_df, None

            symbols = tuple(filtered_df["symbol"].unique())
            history_query = f"""
            SELECT 
                s.symbol AS symbol,
                s.date AS date,
                s.open AS open,
                s.high AS high,
                s.low AS low,
                s.close AS close,
                s.volume AS volume
            FROM eod_prices s
            JOIN (
                SELECT symbol, start_date, end_date
                FROM v_swing_slope_breakout
                WHERE {where_clause}
            ) f ON s.symbol = f.symbol
            AND s.date BETWEEN f.start_date AND f.end_date
            ORDER BY s.symbol, s.date
            """

            print("📄 Fetching associated historical stock data:")
            print(sqlparse.format(history_query, reindent=True, keyword_case="upper"))

            history_df = self.db.execute(history_query).df()
            return filtered_df, history_df

        except Exception as e:
            print("❌ Failed to retrieve breakout candidates or history:")
            print(f"👉 {e}")
            traceback.print_exc()
            return pd.DataFrame(), None

    def query_view(self, view_name: str) -> pd.DataFrame:
        """
        Executes a SELECT * from an existing DuckDB view and returns the result.
        Includes detailed logging and error handling.
        """
        try:
            print(f"🔎 Querying view: {view_name}")
            df = self.db.execute(f"SELECT * FROM {view_name}").df()
            print(f"📋 View '{view_name}' returned {len(df)} rows and {len(df.columns)} columns.")
            return df
        except Exception as e:
            print(f"\n❌ Failed to query view '{view_name}'")
            print("💥 Exception message:")
            print(f"👉 {str(e)}")
            print("🧵 Full traceback:")
            traceback.print_exc()
            return pd.DataFrame()

    def run_strategy(self, sql_file: str) -> pd.DataFrame:
        """
        Runs a SQL strategy from a file and returns the result as a DataFrame.
        Logs the file, parsed SQL, and exception if any.
        """
        try:
            query_path = Path(__file__).parent / sql_file
            if not query_path.exists():
                print(f"❌ SQL file not found: {query_path}")
                return pd.DataFrame()

            query = query_path.read_text()

            print(f"📄 Running strategy from file: {query_path}")

            df = self.db.execute(query).df()
            print(f"📋 df.columns = {df.columns}")
            print(f"✅ Strategy '{sql_file}' returned {len(df)} rows.")
            return df

        except Exception as e:
            print(f"\n❌ Failed to execute SQL in strategy '{sql_file}':\n")
            formatted_query = sqlparse.format(query, reindent=True, keyword_case='upper')
            print("🧠 Query that caused the failure:\n" + formatted_query)
            print("💥 Exception message:")
            traceback.print_exc()
            return pd.DataFrame()