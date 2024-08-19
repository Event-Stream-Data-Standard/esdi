try:
    import polars as pl

    if pl.__version__ < "1.0.0":
        raise ImportError("polars version must be >= 1.0.0 for these utilities")

    import pyarrow as pa

    DF_TYPES = (pl.DataFrame, pl.LazyFrame)

    def _convert_and_validate_schema(
        df: Union[*PL_DF_TYPES],
        mandatory_columns: dict[str, pa.DataType],
        optional_columns: dict[str, pa.DataType],
        do_allow_extra_columns: bool,
        do_cast_types: dict[str, bool],
        do_add_missing_mandatory_fields: bool,
        do_reorder_columns: bool,
        schema_validator: Callable[[list[tuple[str, pa.DataType]]], pa.Schema] | None = None,
    ) -> pa.Table:
        """
        This function converts a DataFrame to an Arrow Table and validates that it has the correct schema.

        Args:
            df: The polars DataFrame or LazyFrame to convert and validate.
            mandatory_columns: A dictionary of mandatory columns and their types.
            optional_columns: A dictionary of optional columns and their types. Optional columns need not be
                present in the DataFrame but, if they are present, they must have the correct type.
            do_allow_extra_columns: Whether to allow extra columns in the DataFrame.
            do_cast_types: Whether it is permissible to cast individual columns to the correct types. This
                parameter must be specified as a dictionary mapping column name to whether it is permissible
                to cast that column.
            do_add_missing_mandatory_fields: Whether it is permissible to add missing mandatory fields to the
                DataFrame with null values. If `False`, any missing values will result in an error.
            do_reorder_columns: Whether it is permissible
            schema_validator: A function that takes a list of tuples of all additional (beyond the mandatory)
                column names and types and returns a PyArrow Schema object for the table, if a valid schema
                exists with the passed columns.
        """

        # If it is not a lazyframe, make it one.
        df = df.lazy()

        schema = df.collect_schema()

        typed_pa_df = pa.Table.from_pylist(
            [], schema=pa.schema(list(mandatory_columns.items()) + list(optional_columns.items()))
        )
        target_pl_schema = pl.from_arrow(typed_pa_df).collect_schema()

        errors = []
        for col in mandatory_columns:
            target_dtype = target_pl_schema[col]
            if col in schema:
                if target_dtype != schema[col]:
                    if do_cast_types[col]:
                        df = df.with_columns(pl.col(col).cast(target_dtype))
                    else:
                        errors.append(
                            f"Column '{col}' must be of type {target_dtype}. Got {schema[col]} instead."
                        )
            elif do_add_missing_mandatory_fields:
                df = df.with_columns(pl.lit(None, dtype=target_dtype).alias(col))
            else:
                errors.append(f"Missing mandatory column '{col}' of type {target_dtype}.")

        for col in optional_columns:
            if col not in schema:
                continue

            target_dtype = target_pl_schema[col]
            if target_dtype != schema[col]:
                if do_cast_types[col]:
                    df = df.with_columns(pl.col(col).cast(target_dtype))
                else:
                    errors.append(
                        f"Optional column '{col}' must be of type {target_dtype} if included. "
                        f"Got {schema[col]} instead."
                    )

        type_specific_columns = set(mandatory_columns.keys()) | set(optional_columns.keys())
        additional_cols = [col for col in schema if col not in type_specific_columns]

        if additional_cols and not do_allow_extra_columns:
            errors.append(f"Found unexpected columns: {additional_cols}")

        if errors:
            raise ValueError("\n".join(errors))

        default_pa_schema = df.head(0).collect().to_arrow().schema

        optional_properties = []
        for col in schema:
            if col in mandatory_columns:
                continue

            if col in optional_columns:
                optional_properties.append((col, optional_columns[col]))
            else:
                optional_properties.append((col, default_pa_schema[col]))

        if schema_validator is not None:
            validated_schema = schema_validator(optional_properties)
        else:
            validated_schema = pa.schema(list(mandatory_columns.items()) + optional_properties)

        schema_order = validated_schema.names

        extra_cols = set(df.columns) - set(schema_order)
        if extra_cols:
            raise ValueError(f"Found unexpected columns: {extra_cols}")

        if schema_order != df.columns:
            if do_reorder_columns:
                df = df.select(schema_order)
            else:
                raise ValueError(f"Column order must be {schema_order}. Got {df.columns} instead.")

        return df.collect().to_arrow().cast(validated_schema)


except ImportError:
    DF_TYPES = tuple()

    def _convert_and_validate_schema(
        df: Any,
        mandatory_columns: dict[str, pa.DataType],
        optional_columns: dict[str, pa.DataType],
        do_allow_extra_columns: bool,
        do_cast_types: dict[str, bool],
        do_add_missing_mandatory_fields: bool,
        do_reorder_columns: bool,
        schema_validator: Callable[[list[tuple[str, pa.DataType]]], pa.Schema] | None = None,
    ) -> pa.Table:
        raise NotImplementedError("polars is not installed")
