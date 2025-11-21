"""
SKU Curves Filtering Asset.

Filters SKUs based on quality thresholds (distinct facings, store presence ratio).
Processes data in parallel by planogram and CDT for performance.

REFACTORED: Uses @graph pattern to persist all intermediate operations.
"""

import re
from collections.abc import Generator
from typing import Any

import dagster as dg
import pandas as pd
from dagster import (
    AssetExecutionContext,
    DynamicOut,
    DynamicOutput,
    Out,
    graph,
    graph_asset,
    op,
)
from pipeline.dagster_types import CurvesFilteredDagsterType


# ═══════════════════════════════════════════════════════════════════════════
# ATOMIC OPS (Modified to accept dict input)
# ═══════════════════════════════════════════════════════════════════════════


@op(out=Out())
def filter_by_distinct_facings(
    cdt_data: dict[str, Any],
) -> pd.DataFrame:
    """Filter SKUs that meet minimum distinct facings requirement."""
    df_data = cdt_data["data"]
    min_distinct_facings = cdt_data["filter_config"]["min_distinct_facings"]

    return (
        df_data.groupby(
            ["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID", "SKU_NBR"],
            as_index=False,
            sort=False,
        )
        .agg(DISTINCT_FACINGS=("HORIZONTAL_FACINGS_NBR", "nunique"))
        .query(f"DISTINCT_FACINGS > {min_distinct_facings}")
    )


@op(out=Out())
def filter_by_store_presence(
    cdt_data: dict[str, Any],
) -> pd.DataFrame:
    """Filter SKUs that meet minimum store presence ratio requirement."""
    df_data = cdt_data["data"]
    sku_store_ratio = cdt_data["filter_config"]["store_ratio_threshold"]

    num_stores = df_data.groupby(
        by=["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID"],
        as_index=False,
        sort=False,
    ).agg(NUM_STORES=("STORE_NBR", "nunique"))

    return (
        df_data.groupby(
            by=[
                "CLUSTER",
                "NEED_STATE",
                "CAT_DSC",
                "PLANOGRAM_DSC",
                "CLUSTER_NS_POG_ID",
                "SKU_NBR",
            ],
            as_index=False,
            sort=False,
        )
        .agg(SKU_PRESENCE=("STORE_NBR", "nunique"))
        .merge(
            right=num_stores,
            on=["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID"],
            how="left",
        )
        .assign(
            NUM_STORES=lambda x: x["NUM_STORES"].replace(0, 1)  # Prevent division by zero
        )
        .assign(SKU_STORE_RATIO=lambda x: x["SKU_PRESENCE"] / x["NUM_STORES"])
        .query(f"SKU_STORE_RATIO >= {sku_store_ratio}")
    )


@op(out=Out())
def identify_valid_skus(
    df_valid_facings: pd.DataFrame,
    df_valid_presence: pd.DataFrame,
) -> pd.DataFrame:
    """Identify SKUs that meet both facings and presence requirements."""
    return df_valid_facings.merge(
        right=df_valid_presence[
            ["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID", "SKU_NBR"]
        ],
        on=["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID", "SKU_NBR"],
        how="inner",
    )[["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID", "SKU_NBR"]]


@op(out=Out())
def normalize_valid_skus(
    cdt_data: dict[str, Any],
    df_valid_skus: pd.DataFrame,
) -> pd.DataFrame:
    """Normalize sales for valid SKUs and mark as SHAPE_ENFORCED=0."""
    df_data = cdt_data["data"]
    df_filtered = df_data.merge(
        right=df_valid_skus,
        on=["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID", "SKU_NBR"],
        how="inner",
    )
    avg_sales = df_filtered.groupby(
        by=["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID", "SKU_NBR"],
        sort=False,
    )["SKU_SALES"].transform("mean")

    return df_filtered.assign(
        SKU_SALES=(df_filtered["SKU_SALES"] / avg_sales.replace(0.0, 1.0)).fillna(
            0.0
        ),  # Prevent division by zero
        SHAPE_ENFORCED=0,
    )


@op(out=Out())
def identify_dropped_curves(
    cdt_data: dict[str, Any],
    df_valid: pd.DataFrame,
) -> pd.DataFrame:
    """Identify curves (CLUSTER_NS_POG_ID) where ALL SKUs failed filters."""
    df_data = cdt_data["data"]
    # Get all unique curves from input data
    all_curves = df_data[
        ["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID"]
    ].drop_duplicates()

    # Get curves that have at least one valid SKU
    valid_curves = df_valid[
        ["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID"]
    ].drop_duplicates()

    # Find dropped curves: curves with NO valid SKUs at all
    dropped_curves = (
        all_curves.merge(
            right=valid_curves,
            on=["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID"],
            how="left",
            indicator=True,
        )
        .query("_merge == 'left_only'")
        .drop(columns=["_merge"])
    )

    return dropped_curves


@op(out=Out())
def normalize_dropped_curve_skus(
    cdt_data: dict[str, Any],
    df_dropped_curves: pd.DataFrame,
) -> pd.DataFrame:
    """Get ALL SKUs for dropped curves and normalize sales, mark as SHAPE_ENFORCED=1."""
    if df_dropped_curves.empty:
        return pd.DataFrame()

    df_data = cdt_data["data"]
    # Merge to get all rows for dropped curves (all SKUs in these curves)
    df_dropped = df_dropped_curves.merge(
        right=df_data,
        on=["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID"],
        how="left",
    )

    # Normalize sales by average sales per SKU
    avg_sales = df_dropped.groupby(
        by=["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CLUSTER_NS_POG_ID", "SKU_NBR"],
        sort=False,
    )["SKU_SALES"].transform("mean")

    return df_dropped.assign(
        SKU_SALES=(df_dropped["SKU_SALES"] / avg_sales.replace(0.0, 1.0)).fillna(0.0),
        SHAPE_ENFORCED=1,
    )


@op(out=Out())
def combine_filtered_results(
    df_valid: pd.DataFrame,
    df_dropped: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine valid SKUs and dropped curve SKUs into final result.

    Valid SKUs: shape_enforced=0 (passed filters)
    Dropped curve SKUs: shape_enforced=1 (entire curve had no valid SKUs)

    Note: Individual invalid SKUs from curves with valid SKUs are NOT included.
    """
    if df_dropped.empty:
        return df_valid
    return pd.concat([df_valid, df_dropped], ignore_index=True)


# ═══════════════════════════════════════════════════════════════════════════
# RESULT WRAPPER OP
# ═══════════════════════════════════════════════════════════════════════════


@op(out={"result": Out()})
def wrap_filtered_result(
    cdt_data: dict[str, Any],
    processed_data: pd.DataFrame,
) -> dict[str, Any]:
    """Wrap processed data back into result dict with metadata."""
    return {
        "pog_key": cdt_data["pog_key"],
        "cdt": cdt_data["cdt"],
        "row_count": len(processed_data),
        "data": processed_data,
    }


# ═══════════════════════════════════════════════════════════════════════════
# GRAPH DEFINITION
# ═══════════════════════════════════════════════════════════════════════════


@graph
def process_cdt_filtered_graph(
    cdt_data: dict[str, Any],
) -> dict[str, Any]:
    """Graph that wires all atomic ops for filtering. Config is in cdt_data."""
    # Filter operations - ops extract what they need from cdt_data
    df_valid_facings = filter_by_distinct_facings(cdt_data)
    df_valid_presence = filter_by_store_presence(cdt_data)
    df_valid_skus = identify_valid_skus(df_valid_facings, df_valid_presence)
    df_valid = normalize_valid_skus(cdt_data, df_valid_skus)
    
    # Handle dropped curves
    df_dropped_curves = identify_dropped_curves(cdt_data, df_valid)
    df_dropped = normalize_dropped_curve_skus(cdt_data, df_dropped_curves)
    
    # Combine results
    processed_data = combine_filtered_results(df_valid, df_dropped)
    
    return wrap_filtered_result(cdt_data, processed_data)


# ═══════════════════════════════════════════════════════════════════════════
# SPLIT & ORCHESTRATOR OPS
# ═══════════════════════════════════════════════════════════════════════════


@op(
    out=DynamicOut(),
    required_resource_keys={"config"},
)
def split_planograms_filtered(
    context: AssetExecutionContext,
    pog_master_preprocessed: pd.DataFrame,
) -> Generator[DynamicOutput[dict[str, str | pd.DataFrame]], None, None]:
    """Split preprocessed POG data by planogram and CDT for parallel SKU filtering."""
    context.log.info(msg=f"🔍 SKU Filtering: Input shape {pog_master_preprocessed.shape}")

    # Extract config once and include in each partition
    config = context.resources.config
    filter_config = {
        "min_distinct_facings": config.sku_filters.min_distinct_facings,
        "store_ratio_threshold": config.sku_filters.store_ratio_threshold,
    }

    planograms: list[str] = pog_master_preprocessed["PLANOGRAM_DSC"].drop_duplicates().tolist()
    context.log.info(msg=f"Discovered {len(planograms)} planograms for SKU filtering")

    total_cdts = 0
    for planogram_dsc in planograms:
        sanitized_pog: str = re.sub(pattern=r"[^A-Za-z0-9_]", repl="_", string=str(planogram_dsc))
        pog_key: str = sanitized_pog
        df_pog: pd.DataFrame = pog_master_preprocessed.loc[
            pog_master_preprocessed["PLANOGRAM_DSC"] == planogram_dsc
        ].copy()

        cdts: list[str] = df_pog["CDT"].unique().tolist()

        for cdt in cdts:
            sanitized_cdt: str = re.sub(pattern=r"[^A-Za-z0-9_]", repl="_", string=str(cdt))
            mapping_key: str = f"{pog_key}_{sanitized_cdt}"
            total_cdts += 1

            df_cdt: pd.DataFrame = df_pog[df_pog["CDT"] == cdt].copy()

            yield DynamicOutput[dict[str, str | pd.DataFrame]](
                value={
                    "pog_key": pog_key,
                    "cdt": cdt,
                    "planogram_dsc": planogram_dsc,
                    "data": df_cdt,
                    "filter_config": filter_config,  # Config included in partition
                },
                mapping_key=mapping_key,
            )

    context.log.info(msg=f"✅ Split into {total_cdts} CDTs for parallel SKU filtering")


@op(out=Out(dagster_type=CurvesFilteredDagsterType))
def collect_filtered(
    context: AssetExecutionContext, filter_results: list[dict[str, Any]]
) -> pd.DataFrame:
    """Collect all filtered SKU data from parallel processing."""
    context.log.info(
        msg=f"📥 Collecting filtered SKU data from {len(filter_results)} CDT partitions"
    )

    all_data: list[pd.DataFrame] = []
    total_rows: int = 0

    for result in filter_results:
        pog_key = result["pog_key"]
        cdt: str = result["cdt"]
        count: int = result["row_count"]
        data: pd.DataFrame = result["data"]

        all_data.append(data)
        total_rows += count
        context.log.info(msg=f"✅ SKU {pog_key}/{cdt}: {count} filtered rows")

    df_all: pd.DataFrame = pd.concat(objs=all_data, ignore_index=True)
    df_all.columns = df_all.columns.str.upper()

    num_valid: int = (df_all["SHAPE_ENFORCED"] == 0).sum()
    num_forced: int = (df_all["SHAPE_ENFORCED"] == 1).sum()

    # Debug: Track total unique SKUs after collection
    total_unique_skus = (
        df_all[["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "SKU_NBR"]]
        .drop_duplicates()
        .shape[0]
    )

    context.log.info(
        msg=f"✅ Collected {total_rows} filtered SKU rows: {num_valid} valid, {num_forced} forced | "
        + f"Total unique SKUs: {total_unique_skus}"
    )
    return df_all


# ═══════════════════════════════════════════════════════════════════════════
# GRAPH ASSET
# ═══════════════════════════════════════════════════════════════════════════


@graph_asset(
    name="curves_filtered_sku",
    group_name="sku_curves",
    kinds={"pandas"},
)
def curves_filtered_sku(pog_master_preprocessed: pd.DataFrame) -> pd.DataFrame:
    """
    Filter SKUs based on quality thresholds with parallel processing by planogram and CDT.

    This asset:
    - Filters SKUs to ensure minimum distinct facings per SKU
    - Filters by store presence ratio (minimum percentage of stores that carry the SKU)
    - Normalizes sales data and adds SHAPE_ENFORCED flag (0=valid, 1=forced)

    Filtering Logic (matches backup/sku_processing.py):
    - Valid SKUs (pass both filters) → kept with SHAPE_ENFORCED=0
    - Invalid SKUs from valid curves → DROPPED entirely (not in output)
    - All SKUs from curves with NO valid SKUs → kept with SHAPE_ENFORCED=1

    Processing:
    - Splits data by planogram and CDT for parallel execution
    - Applies atomic operations through graph pattern (ALL INTERMEDIATES PERSISTED)
    - Collects and combines results with uppercase column names

    Uses config.sku_filters for threshold configuration.
    
    REFACTORED: Now uses @graph pattern to ensure all intermediate ops persist.
    """
    cdt_partitions = split_planograms_filtered(pog_master_preprocessed)
    processed_results = cdt_partitions.map(process_cdt_filtered_graph)
    return collect_filtered(processed_results.collect())


# ═══════════════════════════════════════════════════════════════════════════
# ASSET CHECKS
# ═══════════════════════════════════════════════════════════════════════════


@dg.multi_asset_check(
    specs=[
        dg.AssetCheckSpec(
            name="sku_coverage",
            asset="curves_filtered_sku",
            additional_deps=["pog_master_preprocessed"],
            blocking=False,  # Informational only - invalid SKUs from valid curves are intentionally dropped
        ),
        dg.AssetCheckSpec(
            name="shape_enforced_distribution",
            asset="curves_filtered_sku",
            blocking=True,
        ),
    ]
)
def sku_filtering_checks(
    curves_filtered_sku: pd.DataFrame,
    pog_master_preprocessed: pd.DataFrame,
):
    """Multi-asset checks for SKU filtering validation."""
    # Normalize column names to uppercase for comparison
    # curves_filtered_sku has uppercase columns, but pog_master_preprocessed might have mixed case
    input_df = pog_master_preprocessed.copy()
    input_df.columns = input_df.columns.str.upper()
    output_df = curves_filtered_sku.copy()
    output_df.columns = output_df.columns.str.upper()

    # Check 1: SKU coverage (no drops)
    if input_df.empty or output_df.empty:
        yield dg.AssetCheckResult(
            check_name="sku_coverage",
            asset_key="curves_filtered_sku",
            passed=False,
            metadata={
                "error": "Empty input or output dataframes",
                "input_empty": input_df.empty,
                "output_empty": output_df.empty,
            },
        )
    else:
        # Include CAT_DSC and CDT in comparison since filtering happens per category and CDT
        # A SKU can be valid in one category/CDT but invalid in another
        pk = ["CLUSTER", "NEED_STATE", "CAT_DSC", "PLANOGRAM_DSC", "CDT", "SKU_NBR"]

        # Verify all required columns exist
        missing_input_cols = [col for col in pk if col not in input_df.columns]
        missing_output_cols = [col for col in pk if col not in output_df.columns]

        if missing_input_cols or missing_output_cols:
            yield dg.AssetCheckResult(
                check_name="sku_coverage",
                asset_key="curves_filtered_sku",
                passed=False,
                metadata={
                    "error": "Missing required columns",
                    "missing_input_cols": missing_input_cols,
                    "missing_output_cols": missing_output_cols,
                    "input_columns": list(input_df.columns),
                    "output_columns": list(output_df.columns),
                },
            )
        else:
            input_skus = set(input_df[pk].itertuples(index=False, name=None))
            output_skus = set(output_df[pk].itertuples(index=False, name=None))
            dropped = len(input_skus - output_skus)

            # Informational check - intentional drops are OK
            # Individual invalid SKUs from valid curves are dropped (not in output)
            # Only curves with NO valid SKUs have all their SKUs kept (shape_enforced=1)
            yield dg.AssetCheckResult(
                check_name="sku_coverage",
                asset_key="curves_filtered_sku",
                passed=True,  # Always pass - informational only
                metadata={
                    "input_skus": int(len(input_skus)),
                    "output_skus": int(len(output_skus)),
                    "dropped": int(dropped),
                    "note": "Invalid SKUs from valid curves are intentionally dropped. They will be reintroduced in postprocessing.",
                },
            )

    # Check 2: SHAPE_ENFORCED distribution makes sense
    if curves_filtered_sku.empty:
        yield dg.AssetCheckResult(
            check_name="shape_enforced_distribution",
            asset_key="curves_filtered_sku",
            passed=False,
            metadata={"error": "Empty filtered SKU dataframe"},
        )
    else:
        enforced_counts = curves_filtered_sku["SHAPE_ENFORCED"].value_counts()
        valid_count = int(enforced_counts.get(0, 0))
        forced_count = int(enforced_counts.get(1, 0))
        total = int(len(curves_filtered_sku))

        # Basic sanity check: shouldn't have 100% forced or 0% valid
        reasonable_distribution = (
            valid_count > 0 and forced_count >= 0 and valid_count + forced_count == total
        )

        yield dg.AssetCheckResult(
            check_name="shape_enforced_distribution",
            asset_key="curves_filtered_sku",
            passed=bool(reasonable_distribution),
            metadata={
                "valid_skus": valid_count,
                "forced_skus": forced_count,
                "total_skus": total,
                "valid_percentage": float(round(valid_count / total * 100, 1))
                if total > 0
                else 0.0,
            },
        )


__all__ = ["curves_filtered_sku", "sku_filtering_checks"]
