"""
Módulo de leitura dos dados

"""

import logging
import os
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType

from .config import CLIENTES_SCHEMA, VENDAS_LAYOUT, VENDAS_LINE_LENGTH

logger = logging.getLogger(__name__)

# Clientes
def read_clientes(spark: SparkSession, path: str) -> DataFrame:
    #Lê o csv de clientes
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo de clientes não encontrado: {path}")

    logger.info("Lendo clientes de %s", path)
    df = (
        spark.read
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(CLIENTES_SCHEMA)
        .csv(path)
    )

    # Garante que cliente_id seja tratado como string com padding à esquerda
    # para casar com o formato do arquivo posicional (ex: "1" -> "00001")
    df = df.withColumn(
        "cliente_id",
        F.lpad(F.col("cliente_id").cast("string"), 5, "0"),
    )

    logger.info("Clientes lidos: %d linhas", df.count())
    return df

# Vendas
def read_vendas(spark: SparkSession, path: str) -> DataFrame:
    #Lê o txt de vendas e retorna df tipado
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo de vendas não encontrado: {path}")

    logger.info("Lendo vendas posicionais de %s", path)
    
    # Lê linha a linha como texto
    raw = spark.read.text(path).withColumnRenamed("value", "linha")
    
    # Remove linhas vazias e com tamanho insuficiente
    raw = raw.filter(F.length(F.col("linha")) >= VENDAS_LINE_LENGTH)

    def _slice(col: str, start: int, end: int):
        # Extrair a substring da posição inserida
        length = end - start + 1
        return F.substring(F.col(col), start, length)

    s = VENDAS_LAYOUT
    df = raw.select(
        _slice("linha", *s["venda_id"]).alias("venda_id"),
        _slice("linha", *s["cliente_id"]).alias("cliente_id"),
        _slice("linha", *s["produto_id"]).alias("produto_id"),
        _slice("linha", *s["valor"]).alias("valor_raw"),
        _slice("linha", *s["data_venda"]).alias("data_venda_raw"),
    )

    # Converte valor: 8 dígitos com 2 casas decimais implícitas (00012345 -> 123.45)
    df = df.withColumn(
        "valor",
        (F.col("valor_raw").cast(IntegerType()) / F.lit(100.0)).cast(DecimalType(14, 2)),
    )

    # Converte data_venda de YYYYMMDD para date
    df = df.withColumn(
        "data_venda",
        F.to_date(F.col("data_venda_raw"), "yyyyMMdd"),
    )

    df = df.drop("valor_raw", "data_venda_raw")

    # Remove linhas onde a conversão falhou (valor nulo ou data nula)
    df_valid = df.filter(F.col("valor").isNotNull() & F.col("data_venda").isNotNull())

    total = df.count()
    valid = df_valid.count()
    if valid < total:
        logger.warning(
            "Vendas descartadas por falha de parsing: %d de %d",
            total - valid, total,
        )

    logger.info("Vendas lidas: %d linhas válidas", valid)
    return df_valid