"""
Dagster pipelines - simple pattern: small ops called by big op
"""
from dagster import op, graph_asset, Definitions
from io_manager import pickle_io_manager


# Small helper functions
def calc_stats(data):
    return {"sum": sum(data), "mean": sum(data) / len(data)}


def normalize(data, mean):
    return [x - mean for x in data]


def filter_data(data):
    return [x for x in data if abs(x) <= 2.0]


# Small ops wrapping the functions
@op
def calculate_stats_op(data):
    return calc_stats(data)


@op
def normalize_op(data, mean):
    return normalize(data, mean)


@op
def filter_op(data):
    return filter_data(data)


# Big op that calls the helper functions
@op
def process_data_big_op(raw_data):
    """
    One big op that calls helper functions internally.
    Only this op's output gets persisted.
    """
    print(f"Raw data: {raw_data}")

    # Call the helper functions (not the ops)
    stats = calc_stats(raw_data)
    print(f"Stats: {stats}")

    normalized = normalize(raw_data, stats["mean"])
    print(f"Normalized: {normalized}")

    filtered = filter_data(normalized)
    print(f"Filtered: {filtered}")

    return {"stats": stats, "result": filtered}


@op
def load_data():
    return [1, 2, 3, 4, 5]


# Graph asset calling the big op
@graph_asset
def simple_pipeline():
    """Graph asset that uses the big op"""
    data = load_data()
    return process_data_big_op(data)


defs = Definitions(
    assets=[simple_pipeline],
    resources={
        "io_manager": pickle_io_manager.configured(
            {"base_dir": "/Users/jason/Files/Practice/demo-little-things/dagster-testing/storage"}
        )
    },
)
