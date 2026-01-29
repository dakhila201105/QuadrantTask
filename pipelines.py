from scd2 import apply_scd2
from schema import align_schema
from soft_delete import apply_soft_delete
from transformations import apply_transformations
from dq_checks import run_dq_checks

def run_pipeline(source_df, target_df, config):
    df = align_schema(source_df, config["schema"]["target_columns"])
    df = apply_transformations(df, config.get("transformations", {}))
    run_dq_checks(df, config.get("dq", {}))
    target_df = apply_soft_delete(target_df, df, config["dq"].get("primary_key"))
    target_df = apply_scd2(df, target_df, config.get("scd2", {}))
    return target_df
