# traffikkdata-sync

Fetch data from [Trafikkdata API](https://trafikkdata.atlas.vegvesen.no/om-api) and load it into a DuckDB database using dlt.


## Setup

All traffic registration points in Norway that are operational are fetched.

## Usage

This command will fetch traffic data from January 1, 2024 to January 2, 2024 and save it into a DuckDB database named `my_dataset.duckdb`.

```bash
uv run ./main.py 2024-01-01T00:00:00Z 2024-01-02T00:00:00Z my_dataset
```
