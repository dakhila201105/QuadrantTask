import pandas as pd

def apply_scd2(source_df, target_df, config):
    pk = config["pk"]
    compare_cols = config["compare_columns"]

    today = pd.Timestamp.today().normalize()
    far_future = pd.Timestamp("9999-12-31")

    target_df = target_df.copy()

    # 1️⃣ Get active records only
    active_target = target_df[target_df["is_current"] == True]

    # 2️⃣ Join source with active target
    merged = source_df.merge(
        active_target,
        on=pk,
        how="left",
        suffixes=("_src", "_tgt")
    )

    # 3️⃣ Detect changes (VECTORISED)
    change_mask = False
    for col in compare_cols:
        change_mask |= (
            merged[f"{col}_src"].fillna("##NULL##")
            != merged[f"{col}_tgt"].fillna("##NULL##")
        )

    # Keys that changed
    changed_keys = merged.loc[change_mask, pk].drop_duplicates()

    # If nothing changed → return original target
    if changed_keys.empty:
        return target_df

    # 4️⃣ Expire old records (MERGE INDICATOR – SAFE)
    expire_df = target_df.merge(
        changed_keys,
        on=pk,
        how="left",
        indicator=True
    )

    expire_mask = (expire_df["_merge"] == "both") & (expire_df["is_current"])

    target_df.loc[expire_mask, "is_current"] = False
    target_df.loc[expire_mask, "end_date"] = today

    # 5️⃣ Insert new records
    new_rows = source_df.merge(
        changed_keys,
        on=pk,
        how="inner"
    )

    new_rows["start_date"] = today
    new_rows["end_date"] = far_future
    new_rows["is_current"] = True
    new_rows["is_deleted"] = False

    # 6️⃣ Final result
    final_df = pd.concat([target_df, new_rows], ignore_index=True)

    return final_df
