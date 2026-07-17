"""
Módulo de escrita (em Parquet com compressão snappy).

"""

import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
) -> None:
    #Escreve um df em Parquet particionado por data_venda
    logger.info("Escrevendo Parquet em %s (particionado por data_venda)", path)
    
    (
        df.write
        .mode(mode)
        .option("compression", "snappy")
        .partitionBy("data_venda")
        .parquet(path)
    )
    
    logger.info("Escrita concluída com sucesso em %s", path)