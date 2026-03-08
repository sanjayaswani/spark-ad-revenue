# Ad revenue pipeline

This is a Spark job that joins ad impression and click JSON data and
produces a few reports that are easy to inspect or plug into something else.

## What it does

- App + country metrics: impressions, clicks, revenue and CTR per `(app_id, country_code)`.
- Top advertisers per app + country, ranked by revenue per impression (top 5).
- Median and average user spend per country.

The three outputs are written under `output/`:

- `output/app_country_metrics/`
- `output/top_advertisers/`
- `output/median_user_spend/`

## Project layout

```text
main.py                         # entry point / wiring
ad_revenue_pipeline/
  config.py                     # schemas and a few Spark settings
  quality.py                    # basic data checks
  transformations.py            # aggregations on the joined data
  reader.py                     # JSON reader with corrupt-row handling
data/                           # sample JSON inputs
.mise.toml                      # local Python setup (optional)
requirements.txt                # Python deps
```

## How it works (short version)

At a high level, the job:

1. reads impressions and clicks with an explicit schema,
2. drops corrupt rows instead of failing the whole run,
3. deduplicates both datasets by id,
4. left‑joins impressions to clicks,
5. reuses that joined DataFrame to compute the three reports,
6. writes everything out as JSON.

The aggregation functions in `transformations.py` are kept “dataframe in,
dataframe out”, which makes them easier to test or reuse elsewhere.

## Local setup

If you use [mise](https://mise.jdx.dev), it will create a `.venv` for you
based on `.mise.toml`:

```bash
mise install
pip install -r requirements.txt
```

Without mise, just create a virtualenv for Python 3.9+ and install the
dependencies:

```bash
pip install -r requirements.txt
```

## Running it

Single file:

```bash
python3 main.py \
  --impressions data/impressions.json \
  --clicks data/clicks.json
```

Multiple files:

```bash
python3 main.py \
  --impressions day1.json day2.json \
  --clicks clicks_a.json clicks_b.json
```

## Example cluster submit

On a real cluster I’d usually ship the package as a zip. One basic example:

```bash
zip -r pipeline.zip ad_revenue_pipeline/

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files pipeline.zip \
  main.py \
  --impressions hdfs:///data/raw/impressions/ \
  --clicks hdfs:///data/raw/clicks/ \
  --output-dir hdfs:///data/processed/ad_revenue/
```

For a proper production job I’d tune executor counts, shuffle partitions and
memory based on the real data size and cluster rather than hard‑coding them.
