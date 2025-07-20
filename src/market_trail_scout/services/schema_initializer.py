# ✅ File: `services/schema_initializer.py`
import traceback

class SchemaInitializer:
    def __init__(self, conn):
        self.db = conn

    def init_core_schema(self):
        try:
            print("📦 Setting up database schema...")

            # --- Core Screener Tables ---
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS fundamentals (
                    company_id TEXT PRIMARY KEY,
                    company_name TEXT,
                    sector TEXT,
                    industry TEXT,
                    country TEXT,
                    report_date TEXT,
                    eps_growth_yoy DOUBLE,
                    revenue_growth_yoy DOUBLE,
                    float_shares BIGINT,
                    institutional_ownership_pct DOUBLE,
                    last_updated DATE
                );
            """)

            self.db.execute("""
                CREATE TABLE IF NOT EXISTS symbols (
                    symbol VARCHAR PRIMARY KEY,
                    company_id TEXT,
                    exchange VARCHAR,
                    quote_type VARCHAR,
                    market_cap BIGINT,
                    delisted_date DATE,
                    FOREIGN KEY (company_id) REFERENCES fundamentals(company_id)
                );
            """)

            self.db.execute("""
                CREATE TABLE IF NOT EXISTS eod_prices (
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    PRIMARY KEY (symbol, date),
                    FOREIGN KEY (symbol) REFERENCES symbols(symbol)
                );
            """)
            
            # --- Trade Journal Table ---
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id UUID PRIMARY KEY,
                    account TEXT,
                    account_number TEXT,
                    symbol TEXT,
                    action TEXT,
                    trade_date DATE,
                    settlement_date DATE,
                    quantity DOUBLE,
                    price DOUBLE,
                    total_cost DOUBLE,
                    commission DOUBLE,
                    fees DOUBLE,
                    source TEXT,
                    UNIQUE(symbol, action, trade_date, quantity, price, account_number)
                );
            """)            

            # Add "is_common" column if missing
            col_exists = self.db.execute("""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_name = 'symbols'
                  AND column_name = 'is_common';
            """).fetchone()[0] > 0

            if not col_exists:
                print("➕ Adding 'is_common' column to 'symbols' table...")
                self.db.execute("ALTER TABLE symbols ADD COLUMN is_common BOOLEAN")

            print("🔄 Populating 'is_common' column...")
            self.db.execute("""
                UPDATE symbols
                SET is_common =
                    symbol NOT LIKE '%-P%' AND
                    symbol NOT LIKE '%-UN' AND
                    symbol NOT LIKE '%-WS' AND
                    symbol NOT LIKE '%-RT'
                WHERE is_common IS NULL;
            """)

            # Indexes for screener
            self._ensure_index("idx_symbols_company_id", "CREATE INDEX idx_symbols_company_id ON symbols(company_id);")
            self._ensure_index("idx_eod_prices_symbol", "CREATE INDEX idx_eod_prices_symbol ON eod_prices(symbol);")
            self._ensure_index(
                "idx_unique_trade_conflict_check",
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_trade_conflict_check ON trades (symbol, action, trade_date, quantity, price, account_number);"
            )

            for col, col_type in [
                ("eps_growth_yoy", "DOUBLE"),
                ("revenue_growth_yoy", "DOUBLE"),
                ("float_shares", "BIGINT"),
                ("institutional_ownership_pct", "DOUBLE"),
                ("last_updated", "DATE")
            ]:
                self._ensure_column_exists("fundamentals", col, col_type)

            print("✅ Database schema ready.")

        except Exception:
            print("❌ Failed to initialize database schema:")
            traceback.print_exc()
            raise

    def _ensure_index(self, index_name, create_sql):
        try:
            self.db.execute(create_sql)
            print(f"✅ Created index: {index_name}")
        except Exception as e:
            print(f"ℹ️ Index {index_name} may already exist or failed: {e}")

    def _ensure_column_exists(self, table, column, col_type):
        try:
            exists = self.db.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_name = '{table}' AND column_name = '{column}'
            """).fetchone()[0] > 0

            if not exists:
                print(f"➕ Adding '{column}' to '{table}'...")
                self.db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            else:
                print(f"✅ Column '{column}' already exists in '{table}'")
        except Exception as e:
            print(f"❌ Failed to ensure column '{column}' in '{table}': {e}")
            traceback.print_exc()
