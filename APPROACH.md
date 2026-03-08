# Approach

## First version

I started with one script that:

- read the impressions and clicks JSON files,
- joined them,
- grouped by app and country,
- and printed out a few basic aggregates.

That was just to make sure the data looked sane. Once that worked, I added:

- top 5 advertisers per `(app_id, country_code)` by revenue per impression,
- median user spend per country.

At that point everything still lived in a single file which was fine for quick
experiments but not great to maintain.

## Splitting it into modules

After a bit of back and forth I split the code into:

- `config.py` for schemas and a few Spark settings,
- `reader.py` for JSON reads/writes with simple corrupt‑row handling,
- `quality.py` for small checks that run before any heavy work,
- `transformations.py` for the actual aggregations,
- `main.py` for argument parsing and wiring things together.

Each transformation is a plain function that takes a DataFrame and returns one.
There is no file I/O or Spark session logic inside them, which makes it easier
to follow the flow and to test in isolation if needed.

## Data issues and choices

While playing with the data I ran into a few practical details:

- **Corrupt / malformed JSON** – reads are done with an explicit schema in
PERMISSIVE mode. Bad rows end up in a `_corrupt_record` column and are
logged and dropped before the rest of the pipeline runs.
- **Lineage involving `_corrupt_record`** – simply dropping the column is
not enough, because Spark still keeps it in the plan. To get a “fresh”
DataFrame I use `localCheckpoint(eager=True)` on the cleaned data and
unpersist the original one.
- **Duplicate impression and click IDs** – the sample contains duplicate ids
(both for impressions and clicks). I decided to deduplicate both datasets by
id before joining. That keeps CTR and revenue numbers from being inflated.
- **Negative revenue** – a few rows have negative revenue. I clamp those to
zero close to the read step so the rest of the code doesn’t have to worry
about them.
- **Column naming after the join** – both tables have an `id` column. Before
joining I rename the click id to `click_id` and keep `id` for the impression.
- **Schema enforcement** – I prefer passing an explicit schema instead of
letting Spark infer it. Inference costs an extra pass and can guess wrong
types; an explicit schema also surfaces shape changes early.

## A few performance considerations

This is not tuned to the last percent, but I did make a couple of basic choices:

- All three reports reuse the same joined DataFrame, so I cache it once and
unpersist it after the writes.
- Impression counts use `countDistinct("id")` to avoid double‑counting when a
single impression fans out into multiple joined rows.
- Median user spend is calculated in two steps: first per‑user totals, then a
second aggregation per country. That keeps the query simple and avoids any
tricks with aliases.

## If I had more time

Given more time I would:

- add unit tests around `transformations.py` using small in‑memory DataFrames,
- partition the output by `country_code` to make downstream reads cheaper,
- wire it into a scheduler (cron/Airflow) and push a few basic metrics such as
row counts, run duration and number of corrupt rows.*** End Patch***
