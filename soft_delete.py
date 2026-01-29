def apply_soft_delete(target_df, source_df, pk):
    # Handle both single column (str) and multiple columns (list)
    pk_cols = [pk] if isinstance(pk, str) else pk
    
    source_keys = set(map(tuple, source_df[pk_cols].values))
    mask = ~target_df[pk_cols].apply(tuple, axis=1).isin(source_keys)

    target_df.loc[mask, "is_deleted"] = True
    return target_df
