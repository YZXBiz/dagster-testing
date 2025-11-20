"""
Dagster pipelines - small ops persisted via graph composition
"""
from dagster import op, graph, graph_asset, Definitions
from io_manager import pickle_io_manager


# Small ops
@op
def calculate_stats_op(data):
    return {"sum": sum(data), "mean": sum(data) / len(data)}


@op
def normalize_op(data, stats):
    """Takes stats dict and extracts mean"""
    return [x - stats["mean"] for x in data]


@op
def filter_op(data):
    return [x for x in data if abs(x) <= 2.0]


@op
def load_data():
    return [1, 2, 3, 4, 5]


# Graph that composes the small ops (each will be persisted)
@graph
def process_data_graph(raw_data):
    """
    Graph that chains small ops together.
    Each op output gets persisted!
    """
    stats = calculate_stats_op(raw_data)
    normalized = normalize_op(raw_data, stats)
    return filter_op(normalized)


# Graph asset using the graph
@graph_asset
def simple_pipeline():
    """Graph asset that uses the processing graph"""
    data = load_data()
    return process_data_graph(data)


defs = Definitions(
    assets=[simple_pipeline],
    resources={
        "io_manager": pickle_io_manager.configured(
            {"base_dir": "/Users/jason/Files/Practice/demo-little-things/dagster-testing/storage"}
        )
    },
)
