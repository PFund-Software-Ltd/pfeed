import itertools

from pfeed.enums import DataTool

# Define all possible values for each parameter
COMBOS = {
    'data_tool': list(DataTool.__members__),
    'pipeline_mode': [False, True],
    'use_ray': [False, True],
    'use_prefect': [False, True],
    'use_deltalake': [False, True],
}

# Generate all possible combinations
PARAMS = [
    dict(zip(COMBOS.keys(), values))
    for values in itertools.product(*COMBOS.values())
]