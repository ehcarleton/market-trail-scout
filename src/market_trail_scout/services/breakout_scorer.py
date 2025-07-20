import pandas as pd
import numpy as np
import duckdb
from typing import List
from dataclasses import dataclass
from typing import Optional, List
from scipy.stats import linregress

@dataclass
class BreakoutScore:
    symbol: str
    score: float
    touch_count: int
    tightness_score: float
    volume_contraction: bool
    has_flat_top: bool
    support_slope: float
    stddev_close: float
    base_days: int = 60
    notes: Optional[str] = None
    security_name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None


class BreakoutScorer:
    def __init__(self, db_path: Optional[str] = None, base_days: int = 60):
        self.base_days = base_days

        # Use the same logic as BreakoutScreener to establish the database connection
        if db_path:
            self.db_path = db_path
        else:
            import os
            from pathlib import Path
            from appdirs import user_data_dir

            raw_dir = os.environ.get('SCREENER_ENGINE_DATA_DIR', '')
            expanded_dir = os.path.expandvars(raw_dir)
            data_dir = Path(expanded_dir) if expanded_dir else Path(user_data_dir('ScreenerEngine', 'Miltonstreet'))
            data_dir.mkdir(parents=True, exist_ok=True)
            self.db_path = data_dir / 'stock_data.duckdb'

        self.db = duckdb.connect(str(self.db_path))

    def to_dataframe(self, scores: List[BreakoutScore]) -> pd.DataFrame:
        """
        Converts a list of BreakoutScore objects into a DataFrame.
        """
        return pd.DataFrame([s.__dict__ for s in scores])

    def score_breakout_pattern(self, symbol: str, df: pd.DataFrame, meta: dict = None) -> Optional[BreakoutScore]:
        try:
            df = df.sort_values("date").tail(self.base_days).copy()
            df.reset_index(drop=True, inplace=True)
            if len(df) < self.base_days:
                return None

            close = df["close"]
            volume = df["volume"]

            # Tightness: average range over last 10 days
            range_10d = close.rolling(window=10).apply(lambda x: x.max() - x.min())
            tightness = range_10d.tail(10).mean() / close.iloc[-1]

            # Stddev of close
            stddev_close = close.std() / close.mean()

            # Touch count near resistance
            resistance = close.max()
            touches = close[close >= resistance * 0.985]  # within 1.5% of resistance
            touch_count = len(touches)

            # Volume contraction
            vol_early = volume[:len(volume)//2].mean()
            vol_late = volume[len(volume)//2:].mean()
            volume_contraction = vol_late < vol_early

            # Flat top test
            recent_highs = close.rolling(window=3, center=True).max()
            flat_top = (recent_highs - recent_highs.mean()).abs().mean() / recent_highs.mean() < 0.01

            # Support slope
            lows = df["low"]
            slope, _, _, _, _ = linregress(np.arange(len(lows)), lows)

            # Composite score
            score = 0
            score += max(0, 30 - tightness * 1000)  # tightness under 3% ideal
            score += touch_count * 10
            score += 10 if volume_contraction else 0
            score += 10 if flat_top else 0
            score += max(0, slope * 100)  # upward slope adds
            score = min(score, 100)

            return BreakoutScore(
                symbol=symbol,
                score=round(score, 2),
                touch_count=touch_count,
                tightness_score=round(tightness, 4),
                volume_contraction=volume_contraction,
                has_flat_top=flat_top,
                support_slope=round(slope, 4),
                stddev_close=round(stddev_close, 4),
                base_days=self.base_days,
                security_name=meta.get("security_name") if meta else None,
                sector=meta.get("sector") if meta else None,
                industry=meta.get("industry") if meta else None
            )
        except Exception as e:
            print(f"❌ Error scoring {symbol}: {e}")
            return None

    def evaluate_candidates(self, symbols: List[str]) -> List[BreakoutScore]:
        results = []
        for symbol in symbols:
            print(f"\n🔎 Evaluating symbol: {symbol}")

            # Fetch price data
            query_price = f"""
                SELECT date, open, high, low, close, volume
                FROM eod_prices
                WHERE symbol = '{symbol}'
                ORDER BY date DESC
                LIMIT {self.base_days}
            """
            df = self.db.execute(query_price).df()
            df.columns = [c.lower() for c in df.columns]

            # Fetch metadata from symbols table
            query_meta = f"""
                SELECT f.company_name AS security_name, f.sector, f.industry
                FROM symbols s
                JOIN fundamentals f ON s.company_id = f.company_id
                WHERE s.symbol = '{symbol}'
                LIMIT 1
            """
            meta_row = self.db.execute(query_meta).fetchone()
            meta = dict(zip(["security_name", "sector", "industry"], meta_row)) if meta_row else {}

            print(f"📊 Rows: {len(df)} | Columns: {df.columns.tolist()}")

            score = self.score_breakout_pattern(symbol, df, meta)
            if score:
                results.append(score)
                print(f"✅ Scored {symbol}: {score.score}")
            else:
                print(f"❌ Failed to score {symbol}")

        results.sort(
            key=lambda x: (
                x.sector or "",  # Group by sector (empty string fallback)
                -x.score,  # Then by descending score
                -x.touch_count,  # Tie-breaker: more resistance touches
                x.industry or ""  # Then by industry
            )
        )

        return results
