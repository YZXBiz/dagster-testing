"""
Dagster pipelines demonstrating op composition and graph assets.
"""
from dagster import op, graph, graph_asset, Definitions
from io_manager import pickle_io_manager


# Basic ops
@op
def load_data():
    """Load some initial data"""
    data = [1, 2, 3, 4, 5]
    print(f"Loading data: {data}")
    return data


@op
def transform_data(data):
    """Transform the data"""
    result = [x * 2 for x in data]
    print(f"Transforming data: {data} -> {result}")
    return result


@op
def aggregate_data(transformed_data):
    """Aggregate the transformed data"""
    result = sum(transformed_data)
    print(f"Aggregating data: {transformed_data} -> {result}")
    return result


# Simple graph asset
@graph_asset
def my_pipeline():
    """
    Graph asset that processes data through multiple ops.
    Testing if intermediate op outputs get persisted.
    """
    data = load_data()
    transformed = transform_data(data)
    return aggregate_data(transformed)


# Ops for nested graph composition
@op
def add_ten(data):
    """Add 10 to each element"""
    result = [x + 10 for x in data]
    print(f"Adding 10: {data} -> {result}")
    return result


@op
def multiply_by_three(data):
    """Multiply each element by 3"""
    result = [x * 3 for x in data]
    print(f"Multiplying by 3: {data} -> {result}")
    return result


@op
def filter_large(data):
    """Filter out elements larger than 50"""
    result = [x for x in data if x <= 50]
    print(f"Filtering: {data} -> {result}")
    return result


# Create a graph that composes multiple ops
@graph
def complex_transform(input_data):
    """
    A graph that chains multiple transformation ops.
    This graph can be used within other graphs or graph_assets.
    """
    step1 = add_ten(input_data)
    step2 = multiply_by_three(step1)
    return filter_large(step2)


# Use the nested graph in a graph_asset
@graph_asset
def nested_pipeline():
    """
    Graph asset that uses a nested graph (complex_transform).
    Tests if ops within nested graphs also get persisted.
    """
    data = load_data()
    # Call the nested graph
    processed = complex_transform(data)
    return aggregate_data(processed)


# Define the Dagster definitions
defs = Definitions(
    assets=[my_pipeline, nested_pipeline],
    resources={
        "io_manager": pickle_io_manager.configured(
            {"base_dir": "/Users/jason/Files/Practice/demo-little-things/dagster-testing/storage"}
        )
    },
)
