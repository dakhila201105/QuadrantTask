import pandas as pd

def run_dq_checks(df: pd.DataFrame, config: dict):
    errors = []
    
    if not config:
        return True
    
    pk_columns = config.get("primary_key")
    not_null_columns = config.get("not_null_columns", [])
    data_type_checks = config.get("data_type_checks", {})

    # Primary Key Check
    if pk_columns and df.duplicated(subset=[pk_columns] if isinstance(pk_columns, str) else pk_columns).any():
        errors.append("Duplicate primary keys found")

    # Not Null Check
    if not_null_columns:
        for col in not_null_columns:
            if df[col].isnull().any():
                errors.append(f"Null values found in column: {col}")

    # Data Type Check
    if data_type_checks:
        for col, dtype in data_type_checks.items():
            if df[col].dtype != dtype:
                errors.append(f"Invalid datatype for {col}. Expected {dtype}")

    if errors:
        raise ValueError("DQ Check Failed:\n" + "\n".join(errors))

    return True
