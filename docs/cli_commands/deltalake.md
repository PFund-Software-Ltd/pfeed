# Delta Lake

## Optimize
```bash
# merges small files in Delta Lake tables
pfeed deltalake optimize --storage local
```

## Vacuum
```bash
# cleans up old, unreferenced files in Delta Lake tables, dry-run by default
pfeed deltalake vacuum --storage local

# to actually delete the files, add flag --no-dry-run
pfeed deltalake vacuum --storage local --no-dry-run
```

## Alias
```bash
# using alias "delta"
pfeed delta optimize
pfeed delta vacuum
```

```{seealso}
:class: dropdown
To understand what Delta Lake is, please refer to [Delta Lake](../concepts/delta_lake.md).

As for how to use Delta Lake in `pfeed`, see [Storage](../tutorials/storage.ipynb) and [Integration with Delta Lake](../integrations/deltalake.ipynb).

For more details, please refer to the [Delta Lake documentation](https://delta-io.github.io/delta-rs/why-use-delta-lake/).
```