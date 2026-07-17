"""
Módulo de transformação.

"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

logger = logging.getLogger(__name__)

def join_vendas_clientes(df_vendas: DataFrame, df_clientes: DataFrame) -> DataFrame:
    #Realiza o join entre vendas e clientes pelo cliente_id
    logger.info("Realizando join vendas x clientes")
    return df_vendas.join(df_clientes, on="cliente_id", how="left")

def resumo_por_cliente(df_join: DataFrame) -> DataFrame:
    #Gera o resumo agregado por cliente e por data_venda (para particionamento)
    logger.info("Agregando resumo por cliente")
    resumo = (
        df_join
        .groupBy("cliente_id", "nome", "data_venda")
        .agg(
            F.sum("valor").cast(DecimalType(14, 2)).alias("total_vendas"),
            F.count("venda_id").alias("quantidade_vendas"),
            F.avg("valor").cast(DecimalType(14, 2)).alias("ticket_medio"),
        )
        .orderBy("cliente_id", "data_venda")
    )
    return resumo

def balanco_por_produto(df_vendas: DataFrame) -> DataFrame:
    #Gera o balanço agregado por produto e por data_venda (para particionamento)
    logger.info("Agregando balanço por produto e data")
    balanco = (
        df_vendas
        .groupBy("produto_id", "data_venda")
        .agg(
            F.sum("valor").cast(DecimalType(14, 2)).alias("total_vendas_produto"),
            F.count("venda_id").alias("quantidade_vendas_produto"),
            F.avg("valor").cast(DecimalType(14, 2)).alias("ticket_medio_produto"),
        )
        .orderBy("produto_id", "data_venda")
    )
    return balanco