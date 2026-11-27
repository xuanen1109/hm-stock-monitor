# Seed Radar data integration (research branch)

Two independent data lanes use durable SQLite files on a cloud host mounted volume (not a temporary GitHub Actions runner). No paid service was activated.

## Historical and latest completed session
```sh
python -m pip install -r seed_radar/requirements.txt
python -m seed_radar.market_ingest --db /data/seed/market.sqlite --out /data/seed/export
python -m seed_radar.market_ingest --db /data/seed/market.sqlite --out /data/seed/export --once
```
First command requests approximately five years, using dated TWSE MI_INDEX, TPEx afterTrading/otc and TWSE FMTQIK calendar/index. It skips successfully ingested dates on restart and retains raw JSON and request audit. Second command checks the past seven days; run the first command periodically to repair older gaps. Schedule after 16:00 Asia/Taipei on a persistent host, with no overlapping historical writers. Configure source request rate in consultation with exchange terms. Back up SQLite via its backup API (do not copy a live WAL file alone). Historical endpoint schemas and date checks require real network validation. No historical data downloaded yet by this commit.

The export is compatible with the existing EOD CLI:
```sh
python -m seed_radar scan --bars /data/seed/export/bars.csv --market /data/seed/export/market.csv --engine capital --out /data/seed/research
```
Only run after reviewing coverage.json and ingestion_status.csv: both markets must match the expected latest official session, all necessary history present. Four-digit codes are a provisional universe, not a historical security master; no fabricated limit prices, and missing TWSE limits block simulated entries/exits. TPEx next-day limit fields are shifted to the next official session. TPEx historical volume uses round-lot data. Company actions, actual limit prices for TWSE, suspensions, delisting settlement, complete security types and benchmark remain blockers to credible full-market performance. Missing news/fundamental/sector inputs mean other engines cannot produce validated signals.

## Actual intraday stream
Use a permitted broker/exchange feed, not periodic scraping of public display quotes. TWSE terms: https://www.twse.com.tw/zh/products/information/use.html
Example implementation uses Shioaji 1.7+ read-only tick subscription:
https://sinotrade.github.io/zh/tutor/market_data/streaming/stocks/
```sh
python -m pip install 'shioaji>=1.7,<2'
python -m seed_radar.live_feed --db /data/seed/live.sqlite --symbols /data/seed/symbols.txt --max-symbols 20
```
Set SJ_API_KEY/SJ_SEC_KEY through host secret management with market/data permissions only; never paste into a chat or commit. No CA, order functions or trade subscription. The default 20 is a conservative test scope, not a broker capacity claim. Confirm contract and account limits before expanding to full market. One code per line. Run under a process supervisor with restart backoff. Check live_health (last write <60s) independently; stale/missing quotes or dropped ticks mean no validated intraday signals. No heartbeat based on quotes outside regular session; use official calendar before deployment. Tick retention, aggregation into minute bars, full-market subscription/coverage validation, same-time volume baseline, automated LINE alerts and intraday scoring are not implemented yet. Daily scanner must never treat partial ticks as final daily bars.

## Deployment status
Code only. No host provisioned, credentials accessed, live subscription started, workflow scheduled, or data purchased. Existing monitor unchanged. The current news monitor remains independent; verified PIT snapshots still required for the other engines.
