from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa
    from pfund.entities.products.product_base import BaseProduct


def get_specs_schema(product: BaseProduct) -> pa.Schema | None:
    """Derive the pa.Schema for a product's flattened spec columns.

    Reflects pfund's typed Pydantic spec fields (mixin-contributed fields) into a
    pa.Schema. Returns None when the product has no spec fields (e.g. spot crypto).

    Runtime derivation (vs a hardcoded pfeed-side registry) is the right call
    while pfund's product mixins are still in flux — fields added/removed in
    pfund propagate here without manual updates, no silent drift. Trade-off:
    derived Arrow types follow SchemaBuilder's TypeMapper defaults (Decimal →
    decimal128(38, 10), Enum → string fallback, etc.), so precision/encoding
    choices aren't hand-tuned. Once pfund's product API stabilizes, revisit
    whether to switch to explicit per-asset schemas for tighter control.
    """
    from pfeed.utils.arrow_schema_builder import SchemaBuilder

    allowed_specs: set[str] = product.get_allowed_specs()
    if not allowed_specs:
        return None
    builder = SchemaBuilder()
    return builder.model_to_schema(
        product.__class__,
        include_fields=list(allowed_specs),
    )
