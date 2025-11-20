.PHONY: help dev materialize clean list-storage

help:
	@echo "Available commands:"
	@echo "  make dev          - Start Dagster web UI"
	@echo "  make materialize  - Run the pipeline"
	@echo "  make clean        - Clean storage directory"
	@echo "  make list-storage - List persisted files"

dev:
	uv run dagster dev -m pipelines

materialize:
	uv run dagster asset materialize -m pipelines --select simple_pipeline

clean:
	rm -f storage/*.pickle
	@echo "Storage cleaned"

list-storage:
	@ls -lh storage/ 2>/dev/null || echo "No files in storage"
