"""Testes unitários para o módulo de transformações """

import datetime
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType

from src.transformers import join_vendas_clientes, resumo_por_cliente, balanco_por_produto

def _criar_dados_base(spark: SparkSession):
    # Helper para gerar os DFs de teste
    schema_clientes = StructType([
        StructField("cliente_id", StringType(), True),
        StructField("nome", StringType(), True)
    ])
    df_clientes = spark.createDataFrame(
        [("00001", "Joao"), ("00002", "Maria")], 
        schema_clientes
    )

    schema_vendas = StructType([
        StructField("venda_id", StringType(), True),
        StructField("cliente_id", StringType(), True),
        StructField("produto_id", StringType(), True),
        StructField("valor", DecimalType(14, 2), True),
        StructField("data_venda", DateType(), True),
    ])
    df_vendas = spark.createDataFrame([
        ("V1", "00001", "P1", Decimal("100.00"), datetime.date(2023, 4, 1)),
        ("V2", "00001", "P2", Decimal("50.00"), datetime.date(2023, 4, 1)),
        ("V3", "00002", "P1", Decimal("300.00"), datetime.date(2023, 4, 2)),
    ], schema_vendas)

    return df_vendas, df_clientes

def test_join_vendas_clientes(spark: SparkSession):
    #Garante que o join cruza as informações sem duplicar as vendas
    df_vendas, df_clientes = _criar_dados_base(spark)
    
    df_join = join_vendas_clientes(df_vendas, df_clientes)
    resultados = df_join.collect()
    
    assert len(resultados) == 3
    # Verifica se o nome do cliente 00001 foi associado corretamente à venda 1
    venda_1 = [r for r in resultados if r["venda_id"] == "V1"][0]
    assert venda_1["nome"] == "Joao"

def test_resumo_por_cliente_agregacoes(spark: SparkSession):
    # Garante que soma, média e contagem por cliente/data estão corretas
    df_vendas, df_clientes = _criar_dados_base(spark)
    df_join = join_vendas_clientes(df_vendas, df_clientes)
    
    df_resumo = resumo_por_cliente(df_join)
    resultados = df_resumo.orderBy("cliente_id").collect()
    
    assert len(resultados) == 2
    
    # Valida Cliente 00001 (Joao: V1 de 100 + V2 de 50)
    assert resultados[0]["cliente_id"] == "00001"
    assert resultados[0]["quantidade_vendas"] == 2
    assert resultados[0]["total_vendas"] == Decimal("150.00")
    assert resultados[0]["ticket_medio"] == Decimal("75.00")
    
    # Valida Cliente 00002 (Maria: V3 de 300)
    assert resultados[1]["cliente_id"] == "00002"
    assert resultados[1]["quantidade_vendas"] == 1
    assert resultados[1]["total_vendas"] == Decimal("300.00")
    assert resultados[1]["ticket_medio"] == Decimal("300.00")

def test_balanco_por_produto_agregacoes(spark: SparkSession):
    # Garante que o balanço de produtos agrega os valores corretamente por data
    df_vendas, _ = _criar_dados_base(spark)
    
    df_balanco = balanco_por_produto(df_vendas)
    resultados = df_balanco.orderBy("produto_id", "data_venda").collect()
    
    # Temos P1 no dia 01/04, P1 no dia 02/04, e P2 no dia 01/04
    assert len(resultados) == 3
    
    # Valida Produto P1 no dia 01/04
    assert resultados[0]["produto_id"] == "P1"
    assert str(resultados[0]["data_venda"]) == "2023-04-01"
    assert resultados[0]["total_vendas_produto"] == Decimal("100.00")
    
    # Valida Produto P1 no dia 02/04
    assert resultados[1]["produto_id"] == "P1"
    assert str(resultados[1]["data_venda"]) == "2023-04-02"
    assert resultados[1]["total_vendas_produto"] == Decimal("300.00")