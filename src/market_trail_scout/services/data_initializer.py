import hashlib
import os
import time
import traceback
from datetime import date, timedelta

import pandas as pd
import requests
import yfinance as yf

from services.database_connector import DatabaseConnector
from services.schema_initializer import SchemaInitializer
from services.delay_optimizer import DelayOptimizer


class DataInitializer:
    def __init__(self):
        try:
            self.delay_optimizer = DelayOptimizer()
            self.days_back = int(os.environ.get('SCREENER_ENGINE_DAYS_BACK', '365'))
            self.yp_batch = int(os.environ.get('SCREENER_ENGINE_YP_BATCH', '10'))
            self.fmp_batch = int(os.environ.get('SCREENER_ENGINE_FMP_BATCH', '10'))
            self.symbols_uri = os.environ.get('SCREENER_ENGINE_TICKERS_URI')

            # Setup DB connection
            self.db_connector = DatabaseConnector()
            self.db = self.db_connector.get_connection()

            # Run schema initializer
            SchemaInitializer(self.db).init_core_schema()
        except Exception:
            print("❌ Error during DataInitializer initialization:")
            traceback.print_exc()
            raise

    def update_symbols_list(self):
        base = os.environ.get('SCREENER_ENGINE_FMP_URI')
        key = os.environ.get('SCREENER_ENGINE_FMP_APIKEY')
        if not base or not key:
            print("❌ FMP URI or API key not set. Aborting.")
            return

        try:
            print("📥 Fetching full symbol list from FMP...")
            symbol_list_resp = requests.get(f"{base}/stock/list?apikey={key}", timeout=10)
            symbol_list_resp.raise_for_status()
            symbol_list_json = symbol_list_resp.json()
            print(f"👥 Retrieved {len(symbol_list_json)} symbols from FMP")

            df_full = pd.DataFrame(symbol_list_json)[['symbol', 'name', 'exchangeShortName', 'type']]
            valid_exchanges = ['NYSE', 'NASDAQ', 'NYSEARCA', 'ARCA', 'BATS', 'AMEX']
            df_filtered = df_full[df_full['exchangeShortName'].isin(valid_exchanges)]

            df = df_filtered[['symbol', 'name', 'type']].rename(
                columns={'symbol': 'symbol', 'name': 'Security_Name', 'type': 'quote_type'})
            print(f"🏛️ Filtered to {len(df)} NYSE/NASDAQ-listed eod_prices")

            self.db.register('new_symbols', df)
        except Exception:
            print("❌ Error fetching/processing symbol list:")
            traceback.print_exc()
            return

        try:
            print("⚖️ Comparing with local DB...")
            new_syms = [r[0] for r in self.db.execute("""
                                                      SELECT symbol
                                                      FROM new_symbols
                                                      EXCEPT
                                                      SELECT symbol
                                                      FROM symbols
                                                      """).fetchall()]
            removed_syms = [r[0] for r in self.db.execute("""
                                                          SELECT symbol
                                                          FROM symbols
                                                          WHERE delisted_date IS NULL
                                                          EXCEPT
                                                          SELECT symbol
                                                          FROM new_symbols
                                                          """).fetchall()]
            print(f"🆕 {len(new_syms)} new, 🗑️ {len(removed_syms)} to mark as delisted")
        except Exception:
            print("❌ Error diffing symbol lists:")
            traceback.print_exc()
            return

        for sym in removed_syms:
            try:
                print(f"  🛑 Marking delisted: {sym}")
                self.db.execute(
                    "UPDATE symbols SET delisted_date = ? WHERE symbol = ?",
                    [date.today(), sym]
                )
            except Exception:
                print(f"❌ Error marking {sym} delisted:")
                traceback.print_exc()

        # Build a mapping of symbol to quote_type from new_symbols
        quote_type_map = {
            row[0]: row[1]
            for row in self.db.execute("SELECT symbol, quote_type FROM new_symbols").fetchall()
        }

        self._load_and_save_symbol_metadata(base, key, new_syms, quote_type_map)

        print("🏁 symbol update process completed")

    def fetch_symbol_profiles_from_fmp(self, base, key, batch):
        """
        Fetches company profile information for a batch of symbols from the FMP /profile endpoint.

        Retries up to 3 times if a rate limit (HTTP 429) is encountered. This data includes
        basic metadata, float shares, institutional ownership, and rough EPS/revenue approximations.

        Args:
            base (str): The base URI for FMP API.
            key (str): The API key for FMP.
            batch (List[str]): A list of symbols to fetch.

        Returns:
            List[dict]: List of profile JSON objects, one per symbol (if available).
        """
        tries = 3
        while tries > 0:
            try:
                response = requests.get(f"{base}/profile/{','.join(batch)}?apikey={key}", timeout=15)
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as e:
                if response.status_code == 429:
                    print("⚠️ FMP rate limit hit—waiting 60 seconds.")
                    time.sleep(60)
                    tries -= 1
                else:
                    print(f"❌ HTTP error on batch {batch}: {e}")
                    traceback.print_exc()
                    return []
            except Exception as e:
                print(f"❌ Error fetching batch {batch}: {e}")
                traceback.print_exc()
                return []
        return []

    def parse_symbol_profiles(self, profiles, quote_type_map):
        """
        Parses raw FMP profile data into two structured collections:
        - A list of symbol metadata for the `symbols` table.
        - A dictionary of fundamentals keyed by company_id for the `fundamentals` table.

        This method extracts fields like sector, industry, float shares, and EPS estimates.

        Args:
            profiles (List[dict]): FMP profile API response.
            quote_type_map (Dict[str, str]): Mapping of symbols to quote types.

        Returns:
            Tuple[List[dict], Dict[str, dict]]: Symbol list and fundamental data map.
        """
        new_symbol_data = []
        fundamentals_map = {}

        for profile in profiles:
            sym = profile.get('symbol', '')
            if not sym:
                print("⚠️ Skipping profile with missing symbol:", profile)
                continue

            cik = profile.get('cik')
            company_name = profile.get('companyName')
            country = profile.get('country') or 'US'
            exchange = profile.get('exchangeShortName', '')
            sector = profile.get('sector')
            industry = profile.get('industry')
            market_cap = profile.get('mktCap')
            delisted = bool(profile.get('isDelisted') or profile.get('delistedDate'))
            eps_growth_yoy = profile.get('epsTTM')  # Placeholder: better from ratios-ttm endpoint
            revenue_growth_yoy = profile.get('revenuePerShareTTM')  # Approximate proxy
            float_shares = profile.get('sharesFloat')
            institutional_pct = profile.get('institutionalOwnership')

            if cik:
                company_id = cik
            else:
                raw = f"{company_name}|{country}|{exchange}"
                company_id = hashlib.sha256(raw.encode()).hexdigest()[:16]

            if company_id not in fundamentals_map:
                fundamentals_map[company_id] = {
                    'company_id': company_id,
                    'company_name': company_name,
                    'sector': sector,
                    'industry': industry,
                    'country': country,
                    'report_date': None,
                    'eps_growth_yoy': eps_growth_yoy,
                    'revenue_growth_yoy': revenue_growth_yoy,
                    'float_shares': float_shares,
                    'institutional_ownership_pct': institutional_pct,
                    'symbol': sym
                }

            new_symbol_data.append({
                'symbol': sym,
                'company_id': company_id,
                'exchange': exchange,
                'quote_type': quote_type_map.get(sym, 'unknown'),
                'market_cap': market_cap,
                'delisted_date': None
            })

        return new_symbol_data, fundamentals_map

    def fetch_ratios_ttm_from_fmp(self, base, key, batch):
        """
        Fetches EPS and revenue growth ratios from the FMP /ratios-ttm endpoint for the given symbols.

        This provides more accurate YoY growth rates than the /profile endpoint.

        Args:
            base (str): FMP API base URL.
            key (str): API key for authentication.
            batch (List[str]): List of symbols to query.

        Returns:
            List[dict]: JSON response from the API with ratio metrics.
        """
        try:
            response = requests.get(f"{base}/ratios-ttm/{','.join(batch)}?apikey={key}", timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Error fetching ratios-ttm for batch {batch}: {e}")
            traceback.print_exc()
            return []

    def parse_ratios_ttm(self, ratios_json, fundamentals_map):
        """
        Updates the fundamentals_map in-place with data from the /ratios-ttm API response.

        Overwrites EPS and revenue growth values with more accurate TTM versions
        if available for each symbol.

        Args:
            ratios_json (List[dict]): Parsed API response from /ratios-ttm.
            fundamentals_map (Dict[str, dict]): Existing map of fundamentals keyed by company_id.
        """

        for entry in ratios_json:
            symbol = entry.get("symbol")
            if not symbol:
                continue

            # Find the matching fundamentals entry
            matching_entry = None
            for fund in fundamentals_map.values():
                if fund.get("symbol") == symbol:
                    matching_entry = fund
                    break

            if not matching_entry:
                continue  # Symbol not in current map, skip

            # Update with ratios if available
            eps = entry.get("epsGrowthTTM")
            rev = entry.get("revenueGrowthTTM")

            if eps is not None:
                matching_entry["eps_growth_yoy"] = eps
            if rev is not None:
                matching_entry["revenue_growth_yoy"] = rev

    def insert_symbol_and_fundamental_data(self, new_symbol_data, fundamentals_map, batch_number):
        if fundamentals_map:
            try:
                df3 = pd.DataFrame(fundamentals_map.values())
                self.db.register('fund_insert', df3)
                print(f"✅ Inserting {len(df3)} fundamentals...")
                self.db.execute("""
                                INSERT INTO fundamentals (company_id, company_name, sector, industry, country,
                                                          report_date, eps_growth_yoy, revenue_growth_yoy, float_shares,
                                                          institutional_ownership_pct, last_updated)
                                SELECT company_id,
                                       company_name,
                                       sector,
                                       industry,
                                       country,
                                       report_date,
                                       eps_growth_yoy,
                                       revenue_growth_yoy,
                                       float_shares,
                                       institutional_ownership_pct,
                                       CURRENT_DATE
                                FROM fund_insert
                                WHERE company_id NOT IN (SELECT company_id FROM fundamentals)
                                """)
            except Exception as e:
                print("❌ Failed to insert fundamentals:")
                print(f"👉 {e}")
                traceback.print_exc()
                return

        if new_symbol_data:
            try:
                print(f"✅ Inserting {len(new_symbol_data)} new symbols from batch {batch_number}")
                df2 = pd.DataFrame(new_symbol_data)
                self.db.register('batch_insert', df2)
                self.db.execute("""
                                INSERT INTO symbols (symbol, company_id, exchange, quote_type, market_cap, delisted_date)
                                SELECT symbol, company_id, exchange, quote_type, market_cap, delisted_date
                                FROM batch_insert
                                WHERE symbol NOT IN (SELECT symbol FROM symbols)
                                """)
            except Exception as e:
                print("❌ Failed to insert symbols:")
                print(f"👉 {e}")
                traceback.print_exc()
                return
        else:
            print(f"ℹ️ No new symbols to insert in batch {batch_number}")

    def _load_and_save_symbol_metadata(self, base, key, new_syms, quote_type_map):
        BATCH = self.fmp_batch
        total_batches = (len(new_syms) + BATCH - 1) // BATCH
        for i in range(0, len(new_syms), BATCH):
            batch = new_syms[i:i + BATCH]
            print(f"🔍 Batch {i // BATCH + 1} of {total_batches} metadata fetch: {batch}")
            profiles = self.fetch_symbol_profiles_from_fmp(base, key, batch)
            new_symbol_data, fundamentals_map = self.parse_symbol_profiles(profiles, quote_type_map)
            self.insert_symbol_and_fundamental_data(new_symbol_data, fundamentals_map, i // BATCH + 1)
            time.sleep(1)

    def _safe_yf_download(self, symbols, start=None, end=None, period=None, max_retries=5):
        retry_attempt = 0

        # Automatically fallback to `period="max"` if start is too early
        if start:
            try:
                # Handles string and datetime input
                start_date = pd.to_datetime(start)
                if start_date.year < 1962:
                    print(f"⚠️ Start date {start_date.date()} is too early — switching to period='max'")
                    period = "max"
                    start = None
                    end = None
            except Exception as e:
                print(f"⚠️ Failed to parse start date '{start}': {e} — switching to period='max'")
                period = "max"
                start = None
                end = None

        while retry_attempt < max_retries:
            delay = self.delay_optimizer.get_next_delay()
            print(f"⏳ Waiting {delay:.2f}s before next download attempt (retry {retry_attempt + 1})")
            time.sleep(delay)

            start_time = time.time()
            try:
                print("📡 Starting download...")

                # Validate argument combination
                if period and (start or end):
                    raise ValueError("Cannot use both 'period' and 'start'/'end' with yfinance.")

                # Case 1: period only
                if period:
                    df = yf.download(symbols, period=period, group_by='symbol', threads=False, auto_adjust=True
                                     )

                # Case 2: start + end
                elif start and end:
                    df = yf.download(symbols, start=start, end=end, group_by='symbol', threads=False, auto_adjust=True)

                # Case 3: start only (to now)
                elif start:
                    df = yf.download(symbols, start=start, group_by='symbol', threads=False, auto_adjust=True)

                else:
                    raise ValueError("Must provide either 'period' or 'start' (optionally with 'end').")

                duration = time.time() - start_time
                self.delay_optimizer.record_result(delay=delay, duration=duration, success=True)

                print(f"✅ Success in {duration:.2f}s")
                return df

            except Exception as e:
                duration = time.time() - start_time
                self.delay_optimizer.record_result(delay=delay, duration=duration, success=False)

                print(f"❌ Error (retry {retry_attempt + 1}) — {e}")
                traceback.print_exc()
                retry_attempt += 1

        # All attempts failed
        return pd.DataFrame()

    def _fetch_and_store(self, symbols, start=None, end=None, period=None, cutoff_date=None):
        BATCH = self.yp_batch

        # Determine mode: simple list or (symbol, date) pairs
        is_tuple_mode = symbols and isinstance(symbols[0], tuple)

        if is_tuple_mode:
            # Group symbols by their shared "date" value
            from collections import defaultdict
            batches_by_date = defaultdict(list)
            for symbol, start_date in symbols:
                batches_by_date[start_date].append(symbol)

            for start_date, symbol_batch in batches_by_date.items():
                print(f"📥 Fetching batch for {len(symbol_batch)} symbols with date: {start_date}")
                df = self._safe_yf_download(symbol_batch, start=start_date, end=end)
                self._process_batch(symbol_batch, df, cutoff_date)
        else:
            # Normal mode: symbol list only, using a period or global start
            for i in range(0, len(symbols), BATCH):
                symbol_batch = symbols[i:i + BATCH]
                print(f"📥 Fetching batch {i // BATCH + 1}: {symbol_batch}")

                df = self._safe_yf_download(symbol_batch, start=start, end=end, period=period)
                self._process_batch(symbol_batch, df, cutoff_date)

    def _mark_delisted(self, symbols):
        today = date.today()
        for symbol in symbols:
            try:
                print(f"  🛑 Marking delisted: {symbol}")
                self.db.execute(
                    "UPDATE symbols SET delisted_date = ? WHERE symbol = ? AND delisted_date IS NULL",
                    [today, symbol]
                )
            except Exception as e:
                print(f"❌ Failed to mark {symbol} as delisted: {e}")
                traceback.print_exc()

    def _handle_delisted_if_missing(self, symbol_batch, df):
        today = date.today()

        # Case 1: Entire DataFrame is empty
        if df.empty:
            print(f"⚠️ No data returned for batch: {symbol_batch}")
            self._mark_delisted(symbol_batch)
            return True

        # Case 2: MultiIndex DataFrame but some symbols are missing
        if isinstance(df.columns, pd.MultiIndex):
            returned = set(df.columns.levels[0])
            expected = set(symbol_batch)
            missing = list(expected - returned)
            if missing:
                print(f"⚠️ Missing symbols in batch: {missing}")
                self._mark_delisted(missing)

        # Case 3: Single-symbol flat DataFrame but no data
        elif isinstance(symbol_batch, list) and len(symbol_batch) == 1 and df.empty:
            self._mark_delisted(symbol_batch)
            return True

        return False

    def _process_batch(self, symbol_batch, df, cutoff_date):
        # First check if entire batch is empty or missing symbols
        if self._handle_delisted_if_missing(symbol_batch, df):
            return

        print("🧾 Processing batch...")

        # We'll collect all the cleaned dataframes here
        all_rows = []

        # Go through each symbol in the batch
        for symbol in symbol_batch:
            try:
                # 🔍 Handle multi-symbol vs single-symbol cases from yfinance

                # Case 1: df has MultiIndex columns (i.e., multiple symbols)
                if isinstance(df.columns, pd.MultiIndex):
                    if symbol not in df.columns.levels[0]:
                        print(f"⚠️ symbol '{symbol}' missing from download result")
                        continue
                    # Extract just this symbol's data
                    symbol_df = df[symbol].copy()

                # Case 2: df is a flat DataFrame (i.e., only one symbol returned)
                else:
                    symbol_df = df.copy()

                # Make sure the date is a column, not an index
                symbol_df.reset_index(inplace=True)

                # Add the symbol as a new column (since we lose that info when selecting from df[symbol])
                symbol_df['symbol'] = symbol

                # Rename yfinance columns to match your DuckDB schema
                rename_map = {
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume',
                    'Date': 'date'
                }
                symbol_df.rename(columns=rename_map, inplace=True)

                # These are the exact columns expected by your `eod_prices` table
                expected_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']

                # Ensure no columns are missing — if so, skip this symbol
                missing_cols = [col for col in expected_cols if col not in symbol_df.columns]
                if missing_cols:
                    print(f"⚠️ Skipping '{symbol}' — missing columns: {missing_cols}")
                    continue

                # Trim down to the expected columns only
                symbol_df = symbol_df[expected_cols]

                # Apply a cutoff filter (if provided) to exclude very recent data
                if cutoff_date:
                    symbol_df = symbol_df[symbol_df['date'] < pd.to_datetime(cutoff_date)]

                # Drop any rows with missing/NaN values in required fields
                symbol_df.dropna(subset=expected_cols, inplace=True)

                if symbol_df.empty:
                    print(f"📭 No data for '{symbol}' after filtering")
                    continue

                # Add this symbol's cleaned data to our all_rows list
                all_rows.append(symbol_df)

            except Exception as e:
                print(f"❌ Error processing symbol '{symbol}': {e}")
                traceback.print_exc()

        # If nothing valid was collected, skip this batch
        if not all_rows:
            print("ℹ️ No valid data in batch")
            return

        # Combine all per-symbol DataFrames into one big DataFrame
        combined_df = pd.concat(all_rows, ignore_index=True)

        try:
            # Register the full DataFrame with DuckDB
            self.db.register('combined_df', combined_df)

            # Perform a bulk insert, skipping rows already in eod_prices
            self.db.execute('''
                            INSERT INTO eod_prices (symbol, date, open, high, low, close, volume)
                            SELECT c.symbol, c.date, c.open, c.high, c.low, c.close, c.volume
                            FROM combined_df c
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM eod_prices e
                                WHERE c.symbol = e.symbol AND c.date = e.date
                            )
                            ''')
            print(f"✅ Inserted {len(combined_df)} total rows across batch")
        except Exception as e:
            print(f"❌ Failed batch insert: {e}")
            traceback.print_exc()

        # Show download performance stats
        batch_avg = self.delay_optimizer.get_average_download_duration()
        print(f"⏱️ Batch completed — avg download duration: {batch_avg:.2f} sec")

    def _process_batch_old(self, symbol_batch, df, cutoff_date):
        if self._handle_delisted_if_missing(symbol_batch, df):
            return

        print("🧾 Raw column structure from yfinance:")
        if isinstance(df.columns, pd.MultiIndex):
            print("symbols:", df.columns.levels[0].tolist())
            print("Fields: ", df.columns.levels[1].tolist())
        else:
            print("Flat columns:", df.columns.tolist())

        for symbol in symbol_batch:
            try:
                if symbol not in df.columns.levels[0]:
                    print(f"⚠️ symbol '{symbol}' missing from download result")
                    continue

                symbol_df = df[symbol].copy()
                symbol_df.reset_index(inplace=True)
                # symbol_df['date'] = pd.to_datetime(symbol_df['Date'])
                symbol_df['symbol'] = symbol

                # Rename from yfinance's uppercase to DB lowercase
                rename_map = {
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume',
                    'Date': 'date'
                }
                symbol_df.rename(columns=rename_map, inplace=True)
                # symbol_df.rename(columns={k: v for k, v in rename_map.items() if k in symbol_df.columns}, inplace=True)

                expected_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']
                missing_cols = [col for col in expected_cols if col not in symbol_df.columns]

                if missing_cols:
                    print(f"⚠️ Skipping '{symbol}' — missing columns: {missing_cols}")
                    continue

                symbol_df = symbol_df[expected_cols]

                if cutoff_date:
                    symbol_df = symbol_df[symbol_df['date'] < pd.to_datetime(cutoff_date)]

                symbol_df.dropna(subset=expected_cols, inplace=True)

                if symbol_df.empty:
                    print(f"📭 No data for '{symbol}' after applying cutoff filter")
                    continue

                self.db.register('symbol_df', symbol_df)
                self.db.execute('''
                                INSERT INTO eod_prices (symbol, date, open, high, low, close, volume)
                                SELECT symbol, date, open, high, low, close, volume
                                FROM symbol_df
                                WHERE (symbol, date) NOT IN (SELECT symbol, date FROM eod_prices)
                                ''')

                print(f"✅ Inserted {len(symbol_df)} rows for '{symbol}'")
            except Exception as e:
                print(f"❌ Error processing symbol '{symbol}': {e}")
                traceback.print_exc()

        batch_avg = self.delay_optimizer.get_average_download_duration()
        print(f"⏱️ Batch completed — avg download duration: {batch_avg:.2f} sec")

    def initialize_data(self, exclude_recent_days=0, only_missing=True):
        condition = "s.symbol IS NULL" if only_missing else "1=1"

        query = f"""
            SELECT t.symbol
            FROM symbols t
            LEFT JOIN (
                SELECT DISTINCT symbol FROM eod_prices
            ) s ON t.symbol = s.symbol
            WHERE t.quote_type IN ('Common Stock', 'ETF', 'stock')
              AND t.is_common = TRUE
              AND {condition}
            ORDER BY t.symbol
        """

        symbols = [row[0] for row in self.db.execute(query).fetchall()]
        cutoff_date = (date.today() - pd.Timedelta(days=exclude_recent_days)) if exclude_recent_days else None

        print(
            f"🚀 Initializing full data load for {len(symbols)} symbols. Excluding data >= {cutoff_date}" if cutoff_date else "🚀 Initializing full data load for all dates")

        self._fetch_and_store(symbols, period="max", cutoff_date=cutoff_date)
        print("✅ Initialization complete")

    def initialize_specific_symbols(self, symbols, exclude_recent_days=0):
        if not symbols:
            print("⚠️ No symbols provided for initialization.")
            return

        cutoff_date = (date.today() - pd.Timedelta(days=exclude_recent_days)) if exclude_recent_days else None

        print(f"🚀 Initializing data load for {len(symbols)} specific symbol(s): {symbols}")
        if cutoff_date:
            print(f"📅 Excluding data on or after {cutoff_date}")

        self._fetch_and_store(symbols, period="max", cutoff_date=cutoff_date)
        print("✅ Specific symbol initialization complete")

    def update(self, exclude_recent_days=0):
        today = date.today()
        cutoff_date = today - pd.Timedelta(days=exclude_recent_days) if exclude_recent_days else today

        query = f"""
            SELECT t.symbol, MAX(s.date) AS last_stock_date
            FROM symbols t
            JOIN eod_prices s ON t.symbol = s.symbol
            WHERE t.is_common = TRUE
              AND t.delisted_date IS NULL
              AND t.quote_type = 'stock'
              AND s.date < DATE '{cutoff_date}'
            GROUP BY t.symbol
            HAVING MAX(s.date) > CURRENT_DATE - INTERVAL 30 DAY
            ORDER BY last_stock_date, t.symbol
        """

        symbol_rows = self.db.execute(query).fetchall()

        if not symbol_rows:
            print("✅ No symbols found needing update.")
            return

        print(f"🔄 Preparing to update {len(symbol_rows)} symbols")

        # Prepare list of (symbol, last_date + 1 day) tuples for incremental update
        symbols = [
            (symbol, (last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d'))
            for symbol, last_date in symbol_rows
        ]

        self._fetch_and_store(symbols, end=cutoff_date)
        print("✅ Update complete")

    def update_recent_fundamentals(self, days_back=7):
        """
        Updates the fundamentals table for symbols that have had EOD price activity
        in the last N days using both /profile and /ratios-ttm endpoints.

        This is used to keep fundamentals fresh over time without reloading everything.

        Args:
            days_back (int): How many days of recent activity to check for.
        """
        base = os.environ.get('SCREENER_ENGINE_FMP_URI')
        key = os.environ.get('SCREENER_ENGINE_FMP_APIKEY')
        if not base or not key:
            print("❌ FMP URI or API key not set. Aborting.")
            return

        recent_date = (date.today() - timedelta(days=days_back)).isoformat()
        print(f"🔎 Looking for symbols with EOD data since {recent_date}...")

        # Get active symbols with recent EOD price activity
        active_symbols = [row[0] for row in self.db.execute(f"""
            SELECT DISTINCT symbol
            FROM eod_prices
            WHERE date >= DATE '{recent_date}'
        """).fetchall()]

        if not active_symbols:
            print("⚠️ No active symbols with recent EOD data.")
            return

        print(f"🔄 Updating fundamentals for {len(active_symbols)} symbols...")

        # Lookup quote types for the active symbols
        quote_type_map = {
            row[0]: row[1]
            for row in self.db.execute(f'''
                SELECT symbol, quote_type
                FROM symbols
                WHERE symbol IN ({','.join(['?'] * len(active_symbols))})
            ''', active_symbols).fetchall()
        }

        BATCH = self.fmp_batch
        for i in range(0, len(active_symbols), BATCH):
            batch = active_symbols[i:i + BATCH]
            print(f"📦 Batch {i // BATCH + 1}: {batch}")

            # Fetch and inspect /profile data
            profiles = self.fetch_symbol_profiles_from_fmp(base, key, batch)
            print("📄 /profile data:")
            for p in profiles:
                print(
                    f"  {p.get('symbol')}: epsTTM={p.get('epsTTM')} float={p.get('sharesFloat')} inst_own={p.get('institutionalOwnership')}")

            _, fundamentals_map = self.parse_symbol_profiles(profiles, quote_type_map)
            if not fundamentals_map:
                print("⚠️ No fundamentals parsed from /profile")
                continue

            # Fetch and inspect /ratios-ttm data
            ratios_json = self.fetch_ratios_ttm_from_fmp(base, key, batch)
            print("📊 /ratios-ttm data:")
            for r in ratios_json:
                print(
                    f"  {r.get('symbol')}: epsGrowthTTM={r.get('epsGrowthTTM')} revenueGrowthTTM={r.get('revenueGrowthTTM')}")

            self.parse_ratios_ttm(ratios_json, fundamentals_map)

            # Convert and preview dataframe
            df = pd.DataFrame(fundamentals_map.values())
            df['last_updated'] = pd.Timestamp.today().date()
            print("🧾 Sample updated fundamentals:")
            print(df[['company_id', 'eps_growth_yoy', 'revenue_growth_yoy', 'float_shares',
                      'institutional_ownership_pct']].head())

            self.db.register('fund_update', df)

            try:
                self.db.execute("""
                                UPDATE fundamentals
                                SET eps_growth_yoy              = f.eps_growth_yoy,
                                    revenue_growth_yoy          = f.revenue_growth_yoy,
                                    float_shares                = f.float_shares,
                                    institutional_ownership_pct = f.institutional_ownership_pct,
                                    last_updated                = f.last_updated FROM fund_update f
                                WHERE fundamentals.company_id = f.company_id
                                """)
                print(f"✅ Updated {len(df)} records in batch {i // BATCH + 1}")
            except Exception as e:
                print(f"❌ Failed to update batch {i // BATCH + 1}: {e}")
                traceback.print_exc()

        print("🏁 Fundamental updates complete.")
