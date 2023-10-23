import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import s3fs


def s3_parquet_to_pd(s3_path):
    """Converts parquet files in an s3 bucket to a pandas dataframe

    Parameters
    ----------
    s3_path : str
        Path to s3 bucket containing .parquet files. Consider an example s3_path,
        s3://hb-machine-learning-bucket/pypedream/main/data/gold, containing .parquet 
        files in the structure illustrated below:
            gold
            ├── part-00000-4b7cdc90-5156-46c5-b4c5-305b2b2cc4ef-c000.snappy.parquet
            ├── part-00001-4b7cdc90-5156-46c5-b4c5-305b2b2cc4ef-c000.snappy.parquet
            ├── part-00002-4b7cdc90-5156-46c5-b4c5-305b2b2cc4ef-c000.snappy.parquet
            ├── part-00003-4b7cdc90-5156-46c5-b4c5-305b2b2cc4ef-c000.snappy.parquet
            ├── part-00004-4b7cdc90-5156-46c5-b4c5-305b2b2cc4ef-c000.snappy.parquet

    
    Returns
    _______
    df : pd.DataFrame
        Pandas dataframe containing concatenated data from all the parquet files
        above.

    """
    s3 = s3fs.S3FileSystem()
    df = pq.ParquetDataset(s3_path.strip("/").removeprefix("s3://"), filesystem=s3).read_pandas().to_pandas()
    return df


def calculate_delta(df1, df2, primary_key, ignored_columns=[]):

    # Remove ignored columns
    df1 = df1.drop_duplicates().drop(columns=ignored_columns)
    df2 = df2.drop_duplicates().drop(columns=ignored_columns)
    # print(df1)
    # print(df2)

    # print('dropped')
    # Check schema changes
    # - Common Columns
    common_cols = df2.columns.intersection(df1.columns)

    # - Added columns
    added_cols = [f for f in df2.columns if f not in common_cols]

    # - Removed columns
    removed_cols = [f for f in df1.columns if f not in common_cols]

    # Check row changes
    common_rows = pd.merge(df1, df2, how="outer", indicator=True, on=[primary_key])

    df = pd.DataFrame()
    df[primary_key] = common_rows[primary_key]

    for col in common_cols:
        if col == primary_key:
            continue
        df[col] = np.where(
            common_rows[col + "_x"] != common_rows[col + "_y"],
            common_rows[col + "_x"].astype(str) + " --> " + common_rows[col + "_y"].astype(str),
            common_rows[col + "_x"],
        )

    df.loc[common_rows["_merge"] == "both", "change"] = "changed"
    df.loc[common_rows["_merge"] == "left_only", "change"] = "removed"
    df.loc[common_rows["_merge"] == "right_only", "change"] = "added"

    pattern = ".*-->.*"
    mask = df[common_cols].apply(lambda col: col.astype(str).str.contains(pattern, na=False, case=False)).any(axis=1)
    # print(mask)
    df = df[mask]
    

    return (added_cols,removed_cols,df)


# df1 = pd_from_s3_parquet('s3://hb-machine-learning-bucket/pypedream/main/data/gold/')
# df1 = pd_from_s3_parquet('s3://hb-machine-learning-bucket/pypedream/main/data/gold/')

