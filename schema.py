import pandas as pd

def align_schema(source_df, target_columns):
    return source_df.loc[:, source_df.columns.intersection(target_columns)]

