#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Annotated, Any

import dlt
import typer
from dlt.sources.helpers.requests import Request, Response
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import (
    BasePaginator,
    SinglePagePaginator,
)

logger = logging.getLogger(__name__)
client = RESTClient(
    base_url="https://trafikkdata-api.atlas.vegvesen.no/",
    headers={"User-Agent": "NINAnor-trafikkdata-sync/0.1"},
)

ZonedDateTime = Annotated[
    datetime,
    typer.Argument(
        formats=["%Y-%m-%dT%H:%M:%SZ"],
    ),
]

TRAFFIC_REGISTRATION_POINTS_QUERY = """
{
  trafficRegistrationPoints(searchQuery: {isOperational: true}) {
    id
    name
    location {
      coordinates {
        latLon {
          lat
          lon
        }
      }
    }
    trafficRegistrationType
    dataTimeSpan {
      firstData
      firstDataWithQualityMetrics
    }
    operationalStatus
    registrationFrequency
  }
}
"""

TRAFFIC_DATA_QUERY = """
query TrafficData(
    $point_id: String!,
    $from: ZonedDateTime!,
    $to: ZonedDateTime!,
    $after: String
) {
  trafficData(trafficRegistrationPointId: $point_id) {
    volume {
      byHour(from: $from, to: $to, after: $after) {
        pageInfo {
          endCursor
        }
        edges {
          node {
            from
            to
            total {
              volumeNumbers {
                volume
              }
            }
          }
        }
      }
    }
  }
}
"""


class VegvesenGraphQLPaginator(BasePaginator):
    def __init__(self) -> None:
        super().__init__()
        self.cursor = None

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        page_info = response.json()["data"]["trafficData"]["volume"]["byHour"][
            "pageInfo"
        ]
        self.cursor = page_info["endCursor"]
        self._has_next_page = self.cursor is not None

    def update_request(self, request: Request) -> None:
        request.json["variables"]["after"] = self.cursor  # pyright: ignore[reportOptionalSubscript]


@dlt.resource(
    primary_key="id",
    write_disposition="merge",
)
def traffic_registration_points():
    yield from client.paginate(
        path="",
        method="POST",
        json={"query": TRAFFIC_REGISTRATION_POINTS_QUERY},
        data_selector="data.trafficRegistrationPoints",
        paginator=SinglePagePaginator(),
    )


@dlt.transformer(
    primary_key=[
        "traffic_registration_point_id",
        "from",
        "to",
    ],
    references=[
        {
            "referenced_table": "traffic_registration_points",
            "columns": ["traffic_registration_point_id"],
            "referenced_columns": ["id"],
        }
    ],
    write_disposition="merge",
    parallelized=True,
)
def traffic_data(
    point_id: str, from_timestamp: ZonedDateTime, to_timestamp: ZonedDateTime
):
    logger.debug(
        f"Fetching traffic data for {point_id} from {from_timestamp} to {to_timestamp}"
    )
    pages = client.paginate(
        path="",
        method="POST",
        paginator=VegvesenGraphQLPaginator(),
        data_selector="data.trafficData.volume.byHour.edges[*].node",
        json={
            "variables": {
                "point_id": point_id,
                "from": from_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": to_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "after": None,
            },
            "query": TRAFFIC_DATA_QUERY,
        },
    )
    for page in pages:
        for record in page:
            record["traffic_registration_point_id"] = point_id
            volumeNumbers = record.pop("total")["volumeNumbers"]
            if volumeNumbers is None:
                record["volume"] = None
            else:
                record["volume"] = volumeNumbers["volume"]
            yield record


@dlt.transformer
def get_id(records: list[dict[str, Any]]):
    for record in records:
        logger.debug(f"Processing record ID: {record}")
        yield record["id"]


app = typer.Typer()


@app.callback()
def main(
    from_timestamp: ZonedDateTime,
    to_timestamp: ZonedDateTime,
    pipeline_name: Annotated[str, typer.Argument()] = "trafikkdata_sync",
    debug: bool = typer.Option(False, "--debug", help="Enable debug logging"),
) -> None:
    logging.basicConfig(level=(logging.DEBUG if debug else logging.INFO))
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="duckdb",
        dataset_name="trafikkdata",
        progress="enlighten",
    )
    pipeline.run(traffic_registration_points())
    pipeline.run(
        traffic_registration_points()
        | get_id()
        | traffic_data(from_timestamp, to_timestamp)
    )


def start():
    typer.run(main)


if __name__ == "__main__":
    start()
