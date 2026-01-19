import itertools
from pfeed.enums import DataTool


# ============================================================================
# SHARED TEST PARAMETERS: Used across all data source fixtures
# ============================================================================
# FIXME: use_ray, use_deltalake have been moved to config and io_format respectively
COMBOS = [
    # (param, id)
    ({'use_ray': False, 'use_deltalake': False}, "ray(F)delta(F)"),
    ({'use_ray': True, 'use_deltalake': False}, "ray(T)delta(F)"),
    ({'use_ray': False, 'use_deltalake': True}, "ray(F)delta(T)"),
    ({'use_ray': True, 'use_deltalake': True}, "ray(T)delta(T)"),
]
PARAMS, IDS = map(list, zip(*COMBOS))


# ============================================================================
# bybit_mixed fixture: Tests NON-DEFAULT data_tools
# Cycles through pandas, dask, spark (excludes polars to avoid duplication)
# ============================================================================
def build_mixed_params():
    """Build params that cycle through non-polars data tools."""
    non_polars = [tool for tool in DataTool.__members__ if tool != DataTool.polars]
    tool_cycle = itertools.cycle(non_polars)
    
    mixed_params = []
    mixed_ids = []
    for param, base_id in zip(PARAMS, IDS):
        tool = next(tool_cycle)
        mixed_params.append({'data_tool': tool, **param})
        mixed_ids.append(f"{tool}:{base_id}")
    
    return mixed_params, mixed_ids