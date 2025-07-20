from pathlib import Path
import os
import duckdb
from appdirs import user_data_dir


class DatabaseConnector:
    def __init__(self):
        db_filename = os.environ.get('SCREENER_ENGINE_DATA_NAME', 'screener.duckdb')
        raw_dir = os.environ.get('SCREENER_ENGINE_DATA_DIR', '')
        expanded_dir = os.path.expandvars(raw_dir)
        data_dir = Path(expanded_dir) if expanded_dir else Path(user_data_dir('ScreenerEngine', 'Miltonstreet'))
        data_dir.mkdir(parents=True, exist_ok=True)

        self.duckdb_path = data_dir / db_filename
        self.conn = duckdb.connect(str(self.duckdb_path))

    def get_connection(self):
        return self.conn


