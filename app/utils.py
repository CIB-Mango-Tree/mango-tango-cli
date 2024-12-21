import pyarrow.parquet as pq


def parquet_row_count(filename: str):
    with pq.ParquetFile(filename) as pf:
        return pf.metadata.num_rows
