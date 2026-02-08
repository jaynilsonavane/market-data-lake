# CLAUDE.md — Project Preferences & Rules

## User
- **GitHub**: jaynilsonavane (personal account)
- **Learning goals**: ML fundamentals, software engineering, data engineering

## Git & GitHub Rules
- **NEVER push to org repos** (MyhealthTeam or any other org) — personal repos only
- If an org push is ever explicitly requested, always confirm first with:
  - What changes are being pushed
  - Which org/repo it targets
  - What it will impact
- Commit messages: short, conventional commits (`feat:`, `fix:`, `refactor:`, `docs:`, `chore:`)
- No `Co-Authored-By` lines in commits

## Code Style
- Python with type hints
- Config-driven design (YAML)
- Use `uv` for dependency management

## Project Context
- **market-data-lake**: Financial data lake for stock market analysis
- Architecture: medallion (bronze → silver → gold)
- ETL: ingest (Stooq API) → transform (normalize OHLCV) → load (DuckDB)
- ML work happens in `notebooks/`
- Orchestrator: `main.py`

## Workflow
- Working primarily through notebooks + Claude (no Cursor/IDE)
- Iterative ML learning — EDA → feature engineering → models → evaluation
- Will evolve toward: real-time data, cloud setup, LLM for financial docs
