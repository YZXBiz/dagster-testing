"""
Dagster pipelines - big op calling small ops
"""
from dagster import op, graph_asset, Definitions
from io_manager import pickle_io_manager


# Small ops
@op
def calculate_stats_op(data):
    return {"sum": sum(data), "mean": sum(data) / len(data)}


@op
def normalize_op(data, mean):
    return [x - mean for x in data]


@op
def filter_op(data):
    return [x for x in data if abs(x) <= 2.0]


# Big op that calls the small ops
@op
def process_data_big_op(raw_data):
    """
    Big op that calls small ops directly.
    Testing: will the small op outputs be persisted?
    """
    print(f"Raw data: {raw_data}")

    # Call the small ops
    stats = calculate_stats_op(raw_data)
    print(f"Stats: {stats}")

    normalized = normalize_op(raw_data, stats["mean"])
    print(f"Normalized: {normalized}")

    filtered = filter_op(normalized)
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
