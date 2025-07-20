import pandas as pd
import uuid
from pathlib import Path
from services.database_connector import DatabaseConnector
from services.schema_initializer import SchemaInitializer

class FidelityTradeImporter:
    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)

        # Reuse shared connection
        self.db_connector = DatabaseConnector()
        self.conn = self.db_connector.get_connection()
        SchemaInitializer(self.conn).init_core_schema()

    def import_trades(self):
        print(f"📥 Reading trades from: {self.csv_path}")
        try:
            df = pd.read_csv(self.csv_path, skipinitialspace=True)
        except Exception as e:
            print(f"❌ Failed to read CSV: {e}")
            return

        print(f"🔍 Raw rows read (including non-trades): {len(df)}")

        # Keep only rows where "Run Date" looks like MM/DD/YYYY
        df = df[df["Run Date"].astype(str).str.match(r"^\d{2}/\d{2}/\d{4}$", na=False)]
        print(f"📅 Rows with valid trade dates: {len(df)}")

        # Parse only buy/sell transactions
        def parse_action(action):
            action = str(action).upper()
            if "BOUGHT" in action:
                return "buy"
            elif "SOLD" in action:
                return "sell"
            return None

        df["action"] = df["Action"].apply(parse_action)
        df = df[df["action"].notnull()].copy()
        print(f"📈 Buy/Sell rows detected: {len(df)}")

        if df.empty:
            print("⚠️ No valid trade actions found — nothing to import.")
            return

        def get_col(name, alt=None):
            return df[name] if name in df else df[alt or name]

        # Clean fields
        clean_df = pd.DataFrame({
            "id": [str(uuid.uuid4()) for _ in range(len(df))],
            "account": df["Account"].str.strip(),
            "account_number": df["Account Number"].apply(lambda x: str(x).split(".")[0].strip()),
            "symbol": df["Symbol"].str.upper().str.strip(),
            "action": df["action"],
            "trade_date": pd.to_datetime(df["Run Date"], errors='coerce'),
            "settlement_date": pd.to_datetime(df["Settlement Date"], errors='coerce'),
            "quantity": df["Quantity"].astype(float).abs().round(4),
            "price": pd.to_numeric(get_col("Price", "Price ($)"), errors='coerce').round(4),
            "total_cost": pd.to_numeric(get_col("Amount", "Amount ($)"), errors='coerce'),
            "commission": pd.to_numeric(get_col("Commission", "Commission ($)"), errors='coerce').fillna(0),
            "fees": pd.to_numeric(get_col("Fees", "Fees ($)"), errors='coerce').fillna(0),
            "source": "Fidelity"
        })

        insert_query = """
                       INSERT INTO trades (id, account, account_number, symbol, action, \
                                           trade_date, settlement_date, quantity, price, total_cost, \
                                           commission, fees, source)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(symbol, action, trade_date, quantity, price, account_number) DO NOTHING
            RETURNING id; \
                       """

        rows_to_insert = [
            (
                row.id, row.account, row.account_number, row.symbol, row.action,
                row.trade_date, row.settlement_date, row.quantity, row.price, row.total_cost,
                row.commission, row.fees, row.source
            ) for row in clean_df.itertuples(index=False)
        ]

        inserted_count = 0
        skipped_count = 0
        error_count = 0

        for row in rows_to_insert:
            try:
                result = self.conn.execute(insert_query, row).fetchone()
                if result:
                    inserted_count += 1
                else:
                    skipped_count += 1  # ON CONFLICT skip
            except Exception as e:
                print(f"⚠️ Error inserting row: {e}")
                error_count += 1

        print(
            f"✅ Imported {inserted_count} new trades, skipped {skipped_count} duplicates, and found {error_count} errors.")
