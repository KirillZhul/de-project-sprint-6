import vertica_python
import pandas as pd
import contextlib
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional
from logging import Logger


import logging

logging.basicConfig(level=logging.INFO)

def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    df = pd.read_csv(dataset_path, dtype=type_override)
    num_rows = len(df)
    vertica_conn = vertica_python.connect(
        host='51.250.75.20',
        port=5433,
        user='stv2023081256',
        password = 'aRURinFjTN31QbN'
    )
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '"'
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            logging.info(f"loading rows {start}-{end}") 
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            logging.info("loaded") 
            start += chunk_size + 1

    vertica_conn.close()