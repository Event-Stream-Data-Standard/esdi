"""The data schema for the MEDS format.

Please see the README for more information, including expected file organization on disk, more details on what
this schema should capture, etc.

The data schema.

MEDS data also must satisfy two important properties:

1. Data about a single subject cannot be split across parquet files.
  If a subject is in a dataset it must be in one and only one parquet file.
2. Data about a single subject must be contiguous within a particular parquet file and sorted by time.

Both of these restrictions allow the stream rolling processing
(https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.rolling.html)
which vastly simplifies many data analysis pipelines.

No python type is provided because Python tools for processing MEDS data will often provide their own types.
See https://github.com/EthanSteinberg/meds_reader/blob/0.0.6/src/meds_reader/__init__.pyi#L55 for example.
"""

import pyarrow as pa
from .shared_codes import (
    subject_id_field, time_field, code_field, subject_id_dtype, time_dtype, code_dtype, numeric_value_field,
    numeric_value_dtype
)

from .utils import CustomizableSchemaFntr

MANDATORY_FIELDS = {
    subject_id_field: subject_id_dtype,
    time_field: time_dtype,
    code_field: code_dtype,
    numeric_value_field: numeric_value_dtype,
}

OPTIONAL_FIELDS = {
    "categorical_value": pa.string(),
    "text_value": pa.string(),
}

# This returns a function that will create a data schema with the mandatory fields and any custom fields you
# specify. Types are guaranteed to match optional field types if the names align.
data_schema = CustomizableSchemaFntr(MANDATORY_FIELDS, OPTIONAL_FIELDS)


def convert_and_validate_schema_fntr(
    do_cast_types: bool | dict[str, bool] = True,
    do_add_missing_fields: bool = True,
) -> Callable[[
    df: DF_T,
    schema: pa.Schema,
    if isinstance(df, pa.Table):
        # handle pa.Table
    

    

def get_and_validate_data_schema(df: pl.LazyFrame, stage_cfg: DictConfig) -> pa.Table:
    do_retype = stage_cfg.get("do_retype", True)
    schema = df.collect_schema()
    errors = []
    for col, dtype in MEDS_DATA_MANDATORY_TYPES.items():
        if col in schema and schema[col] != dtype:
            if do_retype:
                df = df.with_columns(pl.col(col).cast(dtype, strict=False))
            else:
                errors.append(f"MEDS Data '{col}' column must be of type {dtype}. Got {schema[col]}.")
        elif col not in schema:
            if col in ("numeric_value", "time") and do_retype:
                df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))
            else:
                errors.append(f"MEDS Data DataFrame must have a '{col}' column of type {dtype}.")

    if errors:
        raise ValueError("\n".join(errors))

    additional_cols = [col for col in schema if col not in MEDS_DATA_MANDATORY_TYPES]

    if additional_cols:
        extra_schema = df.head(0).select(additional_cols).collect().to_arrow().schema
        measurement_properties = list(zip(extra_schema.names, extra_schema.types))
        df = df.select(*MEDS_DATA_MANDATORY_TYPES.keys(), *additional_cols)
    else:
        df = df.select(*MEDS_DATA_MANDATORY_TYPES.keys())
        measurement_properties = []

    validated_schema = data_schema(measurement_properties)
    return df.collect().to_arrow().cast(validated_schema)
