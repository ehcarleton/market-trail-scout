# 🧭 Market Trail Scout: Project Vision & Technical Overview

This document outlines the current and future vision for **Market Trail Scout**, an open source toolkit for screening, tracking, and analyzing trading strategies — starting with classic breakout setups, and expanding over time.

---

## 🎯 Goal

Create a tool that scans the market (starting with the NYSE) nightly to find stocks matching actionable technical setups — beginning with the classic **breakout/consolidation pattern** — and allows users to customize screening parameters through an accessible interface.

Future strategy modules may include trend pullbacks, support/resistance flips, momentum triggers, or volume-based confirmation models.

---

## 📐 Strategy Module: Classic Breakout Pattern (Initial Focus)

This strategy reflects a widely used and well-documented swing trading approach popularized by communities like Felix & Friends.

Key criteria:

- **Trend Filter**: Price must be trading **above a key Simple Moving Average (SMA)** — usually 50, 150, or 200-day
- **Consolidation Zone**: Clear **support/resistance range** tested multiple times over 2–4 months
- **Prior Highs**: Preference for stocks that have made **higher highs in the past**
- **Volume & Momentum**: Breakout should show relative strength or confirmation via volume/momentum
- **Timeframe**: Typically 2–4 month bases, but configurable

---

## 🧰 Technical Architecture (Current & Evolving)

### 1. Screener Engine (Python)

- Built with `pandas`, `pandas_ta`, `yfinance`, and **DuckDB**
- Maintains historical OHLCV data (stored as **Parquet** files)
- Scans thousands of stocks across 6+ months of history
- Identifies setups via:
  - SMA alignment
  - Consolidation zones
  - Base duration and touch counts
- Filters can be adjusted dynamically (via config or future UI)

The screener runs as a **nightly batch job** and exposes results via a **FastAPI REST interface**.

---

### 2. GUI (Planned)

Initial plans considered a `.NET 9` frontend, but the focus has shifted to a **Python-native desktop GUI** using **PyQt or similar frameworks**.

Future GUI goals:
- User-driven filtering and backtesting
- Visual charts of breakout setups
- Trade journaling interface
- Strategy configuration (base tightness, SMA, duration, etc.)

---

### 3. Deployment & Storage

- Screener and GUI can run locally, with optional server deployment
- Data is persisted in **columnar Parquet format** and queried using **DuckDB**
- Backend runs standalone and can be containerized or automated via cron

---

## 🔍 Why It Matters

- Leverages **Python’s ecosystem** for financial data, analysis, and automation
- Encourages transparent logic — no black box signals
- Allows technical traders to customize and track their own filters
- Supports **learning, iteration, and improvement** over time
- Offers a **free, inspectable alternative** to expensive private screeners

---

## 🛣️ Next Steps

- Continue improving the breakout screener logic
- Add trade tracking and journaling features
- Design and prototype the PyQt GUI
- Expand to support additional pattern modules and metrics

Market Trail Scout is designed to evolve — with collaboration, transparency, and continuous refinement as its foundation.
