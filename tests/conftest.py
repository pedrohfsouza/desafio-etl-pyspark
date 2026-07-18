"""Configurações globais e fixtures para os testes"""

import logging
import pytest
from pyspark.sql import SparkSession

# Desativa logs verbosos do Spark durante os testes
logging.getLogger("py4j").setLevel(logging.WARNING)

@pytest.fixture(scope="session")
def spark():
    # Cria uma SparkSession local para ser usada em todos os testes
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("Testes_Desafio_ETL")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()