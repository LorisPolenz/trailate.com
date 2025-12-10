import os
import streamlit as st
from elasticsearch import Elasticsearch
from helpers import logging

logger = logging.get_logger()


@st.cache_resource(ttl=300)
def get_es_client():
    if not os.getenv("ELASTIC_HOST") or not os.getenv("ELASTIC_API_KEY") or not os.getenv("ELASTIC_INDEX"):
        logger.error(
            "env not defined (ELASTIC_HOST, ELASTIC_API_KEY, ELASTIC_INDEX)"
        )
        exit(1)

    es_client = Elasticsearch(
        hosts=os.getenv("ELASTIC_HOST"),
        api_key=os.getenv("ELASTIC_API_KEY")
    )

    if not es_client.info()['tagline']:
        raise Exception("Could not load the client")

    return es_client


@st.cache_data(ttl=120)
def get_most_recent_bucket():
    logger.info("Fetching most recent bucket timestamp")

    es_client = get_es_client()

    res = es_client.search(
        index=os.getenv("ELASTIC_INDEX"),
        query={
            "range": {
                "@timestamp": {
                    "gte": "now-1h"
                }
            }
        },
        aggregations={
            "docs_per_min": {
                "date_histogram": {
                    "field": "@timestamp",
                    "fixed_interval": "1m"
                }
            }
        },
        size=0
    )

    buckets = res['aggregations']['docs_per_min']['buckets']

    for bucket in sorted(buckets, key=lambda x: x['key'], reverse=True):
        if bucket['doc_count'] > 0:
            return bucket['key_as_string']

    raise ValueError("Could not find valid bucket")


@st.cache_data(ttl=600)
def fetch_routes():
    logger.info("Fetching all available routes")
    es_client = get_es_client()

    res = es_client.search(
        index=os.getenv("ELASTIC_INDEX"),
        query={
            "bool": {
                "filter": [
                    {
                        "range": {
                            "@timestamp": {
                                "format": "strict_date_optional_time",
                                "gte": "now-2h"
                            }
                        }
                    }
                ]
            }
        },
        aggregations={
            "route_short_names": {
                "terms": {
                    "field": "route.route_short_name.keyword",
                    "size": 1000
                }
            }
        },
        size=0
    )

    return sorted([x['key'] for x in res['aggregations']['route_short_names']['buckets']])


@st.cache_data(ttl=10)
def fetch_route_delay_historic(trip_id: str):
    logger.info("Fetching historic trip data")
    es_client = get_es_client()

    res = es_client.search(
        index=os.getenv("ELASTIC_INDEX"),
        query={
            "bool": {
                "filter": [
                    {
                        "term": {
                            "trip.trip_id.keyword": trip_id
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": "now-300m"
                            }
                        }
                    }
                ]
            }
        },
        size=1000,
        source=[
            "@timestamp",
            "arrival.delay",
            "departure.delay",
            "stop.stop_name",
            "stop_sequence"
        ],
        sort=[
            {
                "@timestamp": {
                    "order": "asc"
                }
            }
        ]
    )

    # Aggreate docs by stop most recend doc at [-1]
    delays_by_stop = {}
    for hit in res['hits']['hits']:
        source = hit['_source']
        stop_name = source['stop']['stop_name']

        if stop_name in delays_by_stop:
            delays_by_stop[stop_name].append(source)
            continue

        delays_by_stop[stop_name] = [source]

    # Extract delays and sort for stop_sequence
    delays = []
    for key in delays_by_stop:
        stop_sequence = delays_by_stop[key][0]['stop_sequence']

        if not "arrival" in delays_by_stop[key][0]:
            delay_list = [x['departure']['delay'] for x in delays_by_stop[key]]
        else:
            delay_list = [x['arrival']['delay'] for x in delays_by_stop[key]]

        delays.append({
            "delays":  delay_list,
            "stop_sequence": stop_sequence,
            "stop.stop_name": key
        })

    return sorted(delays, key=lambda x: x['stop_sequence'])


@st.cache_data(ttl=30)
def fetch_trip_id(route_short_name: str, trip_headsign: str, stop_name: str, stop_departure_time: str):
    logger.info("Fetching trip_id from paramenters")
    es_client = get_es_client()

    res = es_client.search(
        index=os.getenv("ELASTIC_INDEX"),
        query={
            "bool": {
                "filter": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": "now-24h"
                            }
                        }
                    },
                    {
                        "term": {
                            "route.route_short_name.keyword": route_short_name
                        }
                    },
                    {
                        "term": {
                            "trip_enriched.trip_headsign.keyword": trip_headsign
                        }
                    },
                    {
                        "term": {
                            "stop.stop_name.keyword": stop_name
                        }
                    },
                    {
                        "term": {
                            "stop_time.departure_time.keyword": stop_departure_time
                        }
                    }
                ]
            }
        },
        aggregations={
            "route_ids": {
                "terms": {
                    "field": "trip.trip_id.keyword"
                }
            }
        },
        size=0,
    )

    return sorted([x['key'] for x in res['aggregations']['route_ids']['buckets']])


@st.cache_data(ttl=60)
def fetch_stop_departure_times(route_short_name: str, trip_headsign: str, stop_name: str):
    logger.info("Fetching departure times for a stop")
    es_client = get_es_client()

    res = es_client.search(
        index=os.getenv("ELASTIC_INDEX"),
        query={
            "bool": {
                "filter": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": "now-3h"
                            }
                        }
                    },
                    {
                        "terms": {
                            "route.route_short_name.keyword": [
                                route_short_name
                            ]
                        }
                    },
                    {
                        "term": {
                            "trip_enriched.trip_headsign.keyword": trip_headsign
                        }
                    },
                    {
                        "term": {
                            "stop.stop_name.keyword": stop_name
                        }
                    }
                ]
            }
        },
        aggregations={
            "stop_departure_time": {
                "terms": {
                    "field": "stop_time.departure_time.keyword"
                }
            }
        },
        size=0
    )

    return sorted([x['key'] for x in res['aggregations']['stop_departure_time']['buckets']])


@st.cache_data(ttl=30)
def fetch_route_info(route_short_names: list):
    logger.info("Fetching start times and trip_headsigns for route")
    es_client = get_es_client()

    res = es_client.search(
        index=os.getenv("ELASTIC_INDEX"),
        query={
            "terms": {
                "route.route_short_name.keyword": route_short_names
            }
        },
        aggregations={
            "trip_headsigns": {
                "terms": {
                    "field": "trip_enriched.trip_headsign.keyword"
                }
            },
            "start_times": {
                "terms": {
                    "field": "trip.start_time.keyword"
                }
            }
        },
        size=0
    )

    aggs = res.body['aggregations']

    return {
        "start_times": [x['key'] for x in aggs['start_times']['buckets']],
        "trip_headsigns": [x['key'] for x in aggs['trip_headsigns']['buckets']]
    }


@st.cache_data(ttl=30)
def get_departure_stops(routes: list, trip_headsign):
    logger.info("Fetching departure stops for routes")

    if not routes or not trip_headsign:
        return []

    es_client = get_es_client()

    res = es_client.search(
        index=os.getenv("ELASTIC_INDEX"),
        query={
            "bool": {
                "filter": [
                    {
                        "terms": {
                            "route.route_short_name.keyword": routes
                        }
                    },
                    {
                        "term": {
                            "trip_enriched.trip_headsign.keyword": trip_headsign
                        }
                    }
                ]
            }
        },
        aggs={
            "stops": {
                "terms": {
                    "field": "stop.stop_name.keyword"
                }
            }
        },
        size=0
    )

    return [x['key'] for x in res['aggregations']['stops']['buckets']]
