"""Shared constants for the MEDS schema."""

# Field names and types that are shared across the MEDS schema.
subject_id_field = "subject_id"
time_field = "time"
code_field = "code"
numeric_value_field = "numeric_value"

subject_id_dtype = pa.int64()
time_dtype = pa.timestamp("us")
code_dtype = pa.string()
numeric_value_dtype = pa.float32()

# Canonical codes for select events.
birth_code = "MEDS_BIRTH"
death_code = "MEDS_DEATH"
