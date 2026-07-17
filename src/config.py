"""
Configurações e schemas do pipeline

Define as posições do arquivo de vendas e os schemas 
para leitura dos dados
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
)

# Layout posicional do arquivo vendas.txt
VENDAS_LAYOUT = {
    "venda_id":   (1, 5),   # 5 posições
    "cliente_id": (6, 10),  # 5 posições
    "produto_id": (11, 15), # 5 posições
    "valor":      (16, 23), # 8 posições, 2 casas decimais implícitas
    "data_venda": (24, 31), # 8 posições, YYYYMMDD
}

# Tamanho de cada linha do arquivo
VENDAS_LINE_LENGTH = 31

# Schema do arquivo clientes.csv
CLIENTES_SCHEMA = StructType([
    StructField("cliente_id", StringType(), nullable=False),
    StructField("nome", StringType(), nullable=False),
    StructField("data_nascimento", DateType(), nullable=True),
])