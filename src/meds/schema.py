"""The core schemas for the MEDS format.

Please see the README for more information, including expected file organization on disk, more details on what
each schema should capture, etc.
"""
import datetime
from typing import List, Optional

import pyarrow as pa
from typing_extensions import NotRequired, TypedDict

############################################################

# The data schema.
#
# MEDS data also must satisfy two important properties:
#
# 1. Data about a single subject cannot be split across parquet files.
#   If a subject is in a dataset it must be in one and only one parquet file.
# 2. Data about a single subject must be contiguous within a particular parquet file and sorted by time.

# Both of these restrictions allow the stream rolling processing (see https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.rolling.html), # noqa: E501
# which vastly simplifies many data analysis pipelines.

# We define some codes for particularly important events
birth_code = "MEDS_BIRTH"
death_code = "MEDS_DEATH"

subject_id_field = "subject_id"
time_field = "time"
code_field = "code"

subject_id_dtype = pa.int64()


def data_schema(custom_properties=[]):
    return pa.schema(
        [
            (subject_id_field, subject_id_dtype),
            (time_field, pa.timestamp("us")),  # Static events will have a null timestamp
            (code_field, pa.string()),
            ("numeric_value", pa.float32()),
        ]
        + custom_properties
    )


# No python type is provided because Python tools for processing MEDS data will often provide their own types.
# See https://github.com/EthanSteinberg/meds_reader/blob/0.0.6/src/meds_reader/__init__.pyi#L55 for example.

############################################################

# The label schema. Models, when predicting this label, are allowed to use all data about a subject up to and
# including the prediction time. Exclusive prediction times are not currently supported, but if you have a use
# case for them please add a GitHub issue.

label_schema = pa.schema(
    [
        (subject_id_field, subject_id_dtype),
        # The subject who is being labeled.
        ("prediction_time", pa.timestamp("us")),
        # The time the prediction is made.
        # Machine learning models are allowed to use features that have timestamps less than or equal
        # to this timestamp.
        # Possible values for the label.
        ("boolean_value", pa.bool_()),
        ("integer_value", pa.int64()),
        ("float_value", pa.float64()),
        ("categorical_value", pa.string()),
    ]
)

# Python types for the above schema

Label = TypedDict(
    "Label",
    {
        "subject_id": int,
        "prediction_time": datetime.datetime,
        "boolean_value": Optional[bool],
        "integer_value": Optional[int],
        "float_value": Optional[float],
        "categorical_value": Optional[str],
    },
    total=False,
)


############################################################

# The subject split schema.

train_split = "train"  # For ML training.
tuning_split = "tuning"  # For ML hyperparameter tuning. Also often called "validation" or "dev".
held_out_split = "held_out"  # For final ML evaluation. Also often called "test".

subject_split_schema = pa.schema(
    [
        (subject_id_field, subject_id_dtype),
        ("split", pa.string()),
    ]
)

############################################################

# The dataset metadata schema.
# This is a JSON schema.

dataset_metadata_schema = {
    "type": "object",
    "properties": {
        "dataset_name": {"type": "string"},
        "dataset_version": {"type": "string"},
        "etl_name": {"type": "string"},
        "etl_version": {"type": "string"},
        "meds_version": {"type": "string"},
        "created_at": {"type": "string"},  # Should be ISO 8601
    },
}

# Python type for the above schema

DatasetMetadata = TypedDict(
    "DatasetMetadata",
    {
        "dataset_name": NotRequired[str],
        "dataset_version": NotRequired[str],
        "etl_name": NotRequired[str],
        "etl_version": NotRequired[str],
        "meds_version": NotRequired[str],
        "created_at": NotRequired[str],  # Should be ISO 8601
    },
    total=False,
)

############################################################

# The code metadata schema.
# This is a parquet schema.


# Code metadata must contain at least one row for every unique code in the dataset
def code_metadata_schema(custom_per_code_properties=[]):
    return pa.schema(
        [
            ("code", pa.string()),
            ("description", pa.string()),
            ("parent_codes", pa.list_(pa.string())),
            # parent_codes must be a list of strings, each string being a higher level
            # code that represents a generalization of the provided code. Parent codes
            # can use any structure, but is recommended that they reference OMOP concepts
            # whenever possible, to enable use of more generic labeling functions and OHDSI tools.
            # OMOP concepts are referenced in these strings via the format "$VOCABULARY_NAME/$CONCEPT_NAME".
            # For example: "ICD9CM/487.0" would be a reference to ICD9 code 487.0
        ]
        + custom_per_code_properties
    )


# Python type for the above schema

CodeMetadata = TypedDict("CodeMetadata", {"code": str, "description": str, "parent_codes": List[str]}, total=False)
