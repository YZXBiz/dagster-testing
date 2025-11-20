import pickle
import os
from pathlib import Path
from dagster import (
    op,
    graph_asset,
    Definitions,
    IOManager,
    io_manager,
)


class PickleIOManager(IOManager):
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _get_path(self, context) -> Path:
        """Generate file path based on op/asset name"""
        # For ops inside graph_assets, use step_key; for assets, use asset_key
        try:
            name = context.asset_key.path[-1]
        except:
            # For ops, use step_key which includes the op name
            name = context.step_key.replace(".", "_")
        return self.base_dir / f"{name}.pickle"

    def handle_output(self, context, obj):
        """Save output using pickle"""
        filepath = self._get_path(context)
        with open(filepath, "wb") as f:
            pickle.dump(obj, f)
        context.log.info(f"Saved to {filepath}")

    def load_input(self, context):
        """Load input using pickle"""
        filepath = self._get_path(context.upstream_output)
        with open(filepath, "rb") as f:
            obj = pickle.load(f)
        context.log.info(f"Loaded from {filepath}")
        return obj


@io_manager(config_schema={"base_dir": str})
def pickle_io_manager(context):
    return PickleIOManager(context.resource_config["base_dir"])


# Define simple ops
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


# Create a graph asset using the ops
@graph_asset
def my_pipeline():
    """
    Graph asset that processes data through multiple ops.
    Testing if intermediate op outputs get persisted.
    """
    data = load_data()
    transformed = transform_data(data)
    return aggregate_data(transformed)


# Define the Dagster definitions
defs = Definitions(
    assets=[my_pipeline],
    resources={
        "io_manager": pickle_io_manager.configured(
            {"base_dir": "/Users/jason/Files/Practice/demo-little-things/dagster-testing/storage"}
        )
    },
)
