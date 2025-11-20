# traffikdata-sync

Fetch data from [Trafikkdata API](https://trafikkdata.atlas.vegvesen.no/om-api) and load it into a DuckDB database using DLT.

## Setup

`FROM_DATE` and `TO_DATE` variables in `main.py` define the date range for traffic data retrieval. Adjust these as needed.
Only traffic registration points that are operational are fetched.

## Usage

```bash
uv run ./main.py
```
