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
        """Generate file path based on op name"""
        # Always use the op name from step_key
        # e.g. "simple_pipeline.process_data_big_op" -> "process_data_big_op"
        name = context.step_key.split(".")[-1]
        return self.base_dir / f"{name}.pickle"

    def handle_output(self, context, obj):
        """Save output using pickle"""
        # Save with op name
        filepath = self._get_path(context)
        with open(filepath, "wb") as f:
            pickle.dump(obj, f)
        context.log.info(f"Saved to {filepath}")

        # Also save with asset name if this is an asset
        try:
            asset_name = context.asset_key.path[-1]
            if asset_name != context.step_key.split(".")[-1]:
                asset_filepath = self.base_dir / f"{asset_name}.pickle"
                with open(asset_filepath, "wb") as f:
                    pickle.dump(obj, f)
                context.log.info(f"Also saved to {asset_filepath}")
        except:
            pass

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
