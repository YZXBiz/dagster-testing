"""
Custom IO Manager for persisting op/asset outputs using pickle.
"""
import pickle
from pathlib import Path
from dagster import IOManager, io_manager


class PickleIOManager(IOManager):
    """
    IO Manager that persists outputs to disk using pickle serialization.

    Handles both:
    - Asset outputs (uses asset_key)
    - Op outputs within graphs (uses step_key)
    """

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _get_path(self, context) -> Path:
        """Generate file path based on op/asset name"""
        # For ops inside graph_assets, use step_key; for assets, use asset_key
        try:
            name = context.asset_key.path[-1]
        except:
            # For ops, extract just the op name (last part of step_key)
            # e.g. "simple_pipeline.load_data" -> "load_data"
            name = context.step_key.split(".")[-1]
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
    """Factory function to create PickleIOManager instances"""
    return PickleIOManager(context.resource_config["base_dir"])
