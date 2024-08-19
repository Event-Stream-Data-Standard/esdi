"""Utilities for specifying and working with MEDS schemas."""

import pyarrow as pa
from typing import Callable

SCHEMA_DICT_T = dict[str, pa.DataType]

from .polars_support import (
    DF_TYPES as PL_DF_TYPES, 
    _convert_and_validate_schema as pl_convert_and_validate_schema,
)

from .pyarrow_support import (
    DF_TYPES as PA_DF_TYPES,
    _convert_and_validate_schema as pa_convert_and_validate_schema,
)

PA_DF_TYPES = (pa.Table,)

__DF_VALIDATORS = [
    (PL_DF_TYPES, pl_convert_and_validate_schema),
    #(PD_DF_TYPES, pd_convert_and_validate_schema),
]

DF_T = TypeVar("DF_T", *(PA_DF_TYPES + PL_DF_TYPES))

def convert_and_validate_schema(
    df: DF_T,
    schema: pa.Schema | CustomizableSchemaFntr,
    do_cast_types: bool | dict[str, bool] = True,
    do_add_missing_mandatory_fields: bool = True,
    do_reorder_columns: bool = True,
) -> pa.Table:
    """TODO
    """

    match schema:
        case CustomizableSchemaFntr():
            mandatory_columns = schema.mandatory_fields
            optional_columns = schema.optional_fields
            do_allow_extra_columns = True
        case pa.Schema():
            mandatory_columns = {k: dt for k, dt in zip(schema.names, schema.types)}
            optional_columns = {}
            do_allow_extra_columns = False
        case _:
            raise ValueError("Schema must be a CustomizableSchemaFntr or a pa.Schema.")

    all_columns = set(mandatory_columns.keys()) | set(optional_columns.keys())
    if isinstance(do_cast_types, bool):
        do_cast_types = {col: do_cast_types for col in all_columns}
    elif not isinstance(do_cast_types, dict):
        raise ValueError("do_cast_types must be a bool or a dict.")
    elif not all(col in all_columns for col in do_cast_types):
        missing_cols = sorted(list(set(all_columns) - set(do_cast_types)))
        raise ValueError(
            "If it is a dict, do_cast_types must have a key for every column in the schema. Missing "
            f"columns: {', '.join(missing_cols)}."
        )
    elif not all(type(v) is bool for v in do_cast_types.values()):
        invalid_types = {col: v for col, v in do_cast_types.items() if type(v) is not bool}
        raise ValueError(f"do_cast_types values must be bools. Got invalid types: {invalid_types}.")

    kwargs = {
        "mandatory_columns": mandatory_columns,
        "optional_columns": optional_columns,
        "do_allow_extra_columns": do_allow_extra_columns,
        "do_cast_types": do_cast_types,
        "do_add_missing_mandatory_fields": do_add_missing_mandatory_fields,
        "do_reorder_columns": do_reorder_columns
    }

    for df_types, validator_fn in __DF_VALIDATORS:
        if isinstance(df, df_types):
            return validator_fn(df, **kwargs)

    if not isinstance(df, PA_DF_TYPES):
        raise ValueError(f"DataFrame must be one of the allowed types. Got {type(df)}.")

    return pa_convert_and_validate_schema(df, **kwargs)


class CustomizableSchemaFntr(object):
    def __init__(self, mandatory_fields: SCHEMA_DICT_T, optional_fields: SCHEMA_DICT_T):
        """Returns a member object that can be called to create a schema with custom properties.

        The returned schema will contain, in entry order, all the fields specified in `mandatory_fields`
        followed by any custom properties specified in the function call. Any custom properties must not share
        a name with a mandatory field, and must have the correct type if they share a name with an optional
        field.

        Args:
            mandatory_fields: The mandatory fields for the schema.
            optional_fields: The optional fields for the schema.

        Returns:
            A function that can be used to create a schema with custom properties.

        Raises:
            ValueError: If a custom property conflicts with a mandatory field or is an optional field but has the
                wrong type.

        Examples:
            >>> raise NotImplementedError("doctests should fail")
        """
        self.mandatory_fields = mandatory_fields
        self.optional_fields = optional_fields

    def __call__(
        self, custom_properties: list[tuple[str, pa.DataType]] | SCHEMA_DICT_T | None = None
    ) -> pa.Schema:
        """Returns the final, cutomized schema for the specified format."""

        for field, dtype in custom_properties:
            if field in self.mandatory_fields:
                raise ValueError(f"Custom property {field} conflicts with a mandatory field.")
            if field in self.optional_fields and dtype != self.optional_fields[field]:
                raise ValueError(f"Custom property {field} must be of type {optional_fields[field]}.")

        if custom_properties is None:
            custom_properties = []
        elif isinstance(custom_properties, dict):
            custom_properties = list(custom_properties.items())

        return pa.schema(list(self.mandatory_fields.items()) + custom_properties)
