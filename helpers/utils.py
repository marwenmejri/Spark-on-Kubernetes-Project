import pandas as pd


# Metadata schema example
METADATA_SCHEMA = ["schema_id", "column_names", "data_types", "ingestion_time", "source_file", "file_size", "row_count"]


def csv_has_header(file_path):
    """
        Determine if a CSV file has a header.

        Parameters:
        - file_path (str): The path to the CSV file to be analyzed.

        Returns:
        - bool: True if the CSV file has a header, False otherwise.
    """
    df_inferred = pd.read_csv(file_path, nrows=5)
    df_first_row = pd.read_csv(file_path, header=None, nrows=1)
    inferred_dtypes = df_inferred.dtypes
    first_row_dtypes = df_first_row.dtypes
    for inferred_dtype, first_row_dtype in zip(inferred_dtypes, first_row_dtypes):
        if inferred_dtype != first_row_dtype:
            return True
    if all(df_first_row.applymap(lambda x: isinstance(x, str)).iloc[0]):
        return True
    return False
