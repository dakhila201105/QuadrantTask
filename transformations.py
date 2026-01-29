import pandas as pd

def apply_transformations(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Apply transformations such as:
    - uppercase
    - column selection
    - filtering
    - joins (single or multiple)

    Config-driven and dataset-agnostic
    """

    result_df = df

    # 1️⃣ Uppercase columns
    for col in config.get("uppercase", []):
        if col in result_df.columns:
            result_df[col] = result_df[col].astype(str).str.upper()

    # 2️⃣ Filtering
    if "filter" in config and config["filter"]:
        result_df = result_df.query(config["filter"])

    # 3️⃣ Joins (IMPORTANT – industry level)
    for join_cfg in config.get("joins", []):
        result_df = result_df.merge(
            join_cfg["df"],                # dataframe to join
            on=join_cfg.get("on"),         # common column(s)
            left_on=join_cfg.get("left_on"),
            right_on=join_cfg.get("right_on"),
            how=join_cfg.get("how", "left"),
            suffixes=join_cfg.get("suffixes", ("", "_ref"))
        )

    # 4️⃣ Select required columns (always last)
    if "select" in config and config["select"]:
        result_df = result_df[config["select"]]

    return result_df
