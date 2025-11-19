#!/usr/bin/env python3

"""Main script."""

import logging
import pathlib

import dlt
import environ
from dlt.extract.source import DltSource
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig

env = environ.Env()
BASE_DIR = pathlib.Path(__file__).parent
environ.Env.read_env(str(BASE_DIR / ".env"))

DEBUG = env.bool("DEBUG", default=False)  # pyright: ignore[reportArgumentType]

logging.basicConfig(level=(logging.DEBUG if DEBUG else logging.INFO))

logger = logging.getLogger(__name__)


@dlt.source()
def traffikdata_source() -> DltSource:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://trafikkdata-api.atlas.vegvesen.no/",
        },
        "resource_defaults": {
            "endpoint": {
                "method": "POST",
            },
            "write_disposition": "merge",
        },
        "resources": [
            {
                "name": "traffic_registration_points",
                "endpoint": {
                    "path": "",
                    "data_selector": "data.trafficRegistrationPoints",
                    "paginator": {
                        "type": "single_page",
                    },
                    "json": {
                        "query": """
                            {{
                                trafficRegistrationPoints(
                                    searchQuery: {{ isOperational: true }},
                                ) {{
                                    id
                                    name
                                    location {{
                                        coordinates {{
                                            latLon {{
                                                lat
                                                lon
                                            }}
                                        }}
                                    }}
                                    trafficRegistrationType
                                    dataTimeSpan {{
                                        firstData
                                        firstDataWithQualityMetrics
                                    }}
                                    operationalStatus
                                    registrationFrequency
                                }}
                            }}
                        """,
                    },
                },
                "primary_key": "id",
            },
        ],
    }
    return rest_api_source(config)


def start() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="traffikdata_pipeline",
        destination="duckdb",
        dataset_name="traffikdata",
    )
    _load_info = pipeline.run(traffikdata_source())


if __name__ == "__main__":
    start()
