import pandas as pd
from collections import deque
from pathlib import Path
from services.database_connector import DatabaseConnector
from services.schema_initializer import SchemaInitializer


class FifoTradeMatcher:
    def __init__(self):
        # Reuse shared DuckDB connection
        self.db_connector = DatabaseConnector()
        self.conn = self.db_connector.get_connection()
        SchemaInitializer(self.conn).init_core_schema()

    def fetch_trades(self):
        query = """
            SELECT symbol, account_number, action, trade_date, quantity, price
            FROM trades
            ORDER BY symbol, account_number, trade_date
        """
        return self.conn.execute(query).fetchdf()

    def match_fifo_trades(self, df: pd.DataFrame) -> pd.DataFrame:
        open_lots = {}
        closed_trades = []

        for _, row in df.iterrows():
            key = (row['symbol'], row['account_number'])
            if key not in open_lots:
                open_lots[key] = deque()

            if row['action'].lower() == 'buy':
                open_lots[key].append({
                    'date': row['trade_date'],
                    'quantity': row['quantity'],
                    'price': row['price']
                })
            elif row['action'].lower() == 'sell':
                qty = row['quantity']
                while qty > 0 and open_lots[key]:
                    lot = open_lots[key][0]
                    matched_qty = min(qty, lot['quantity'])

                    closed_trades.append({
                        'symbol': row['symbol'],
                        'account_number': row['account_number'],
                        'buy_date': lot['date'],
                        'sell_date': row['trade_date'],
                        'quantity': matched_qty,
                        'buy_price': lot['price'],
                        'sell_price': row['price'],
                        'cost_basis': round(matched_qty * lot['price'], 2),
                        'proceeds': round(matched_qty * row['price'], 2),
                        'gain': round(matched_qty * (row['price'] - lot['price']), 2)
                    })

                    qty -= matched_qty
                    lot['quantity'] -= matched_qty
                    if lot['quantity'] == 0:
                        open_lots[key].popleft()

        return pd.DataFrame(closed_trades)

    def run(self) -> pd.DataFrame:
        df = self.fetch_trades()
        matched = self.match_fifo_trades(df)
        return matched
