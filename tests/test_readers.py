"""Testes unitários para o módulo de leitura """

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from src.readers import read_clientes, read_vendas

# Testes da leitura de clientes

def test_read_clientes_com_padding(spark: SparkSession, tmp_path):
    #Garante que a leitura de clientes faz o lpad de 5 zeros corretamente
    csv_file = tmp_path / "clientes.csv"
    csv_file.write_text(
        "cliente_id,nome,data_nascimento\n1,Joao Silva,1990-01-01\n20,Maria Sousa,1985-05-12\n", 
        encoding="utf-8"
    )
    
    df = read_clientes(spark, str(csv_file))
    resultados = df.collect()
    
    assert len(resultados) == 2
    assert resultados[0]["cliente_id"] == "00001"
    assert resultados[1]["cliente_id"] == "00020"
    assert resultados[0]["nome"] == "Joao Silva"
    assert resultados[1]["nome"] == "Maria Sousa"

def test_read_clientes_arquivo_nao_encontrado(spark: SparkSession):
    #Garante que a função gera FileNotFoundError se o CSV não existir
    caminho_invalido = "caminho/falso/clientes.csv"
    with pytest.raises(FileNotFoundError, match="Arquivo de clientes não encontrado"):
        read_clientes(spark, caminho_invalido)


# Testes da leitura de Vendas
def test_read_vendas_posicional(spark: SparkSession, tmp_path):
    # Garante a leitura posicional correta e a conversão de tipos
    txt_file = tmp_path / "vendas.txt"
    # Layout: venda(5), cliente(5), produto(5), valor(8), data(8)
    # Valor 00015000 -> 150.00 | Data 20230401 -> 2023-04-01
    linha_valida = "V000100001P00010001500020230401\n"
    txt_file.write_text(linha_valida, encoding="utf-8")
    
    df = read_vendas(spark, str(txt_file))
    resultados = df.collect()
    
    assert len(resultados) == 1
    row = resultados[0]
    
    assert row["venda_id"] == "V0001"
    assert row["cliente_id"] == "00001"
    assert row["produto_id"] == "P0001"
    assert float(row["valor"]) == 150.00
    assert str(row["data_venda"]) == "2023-04-01"

def test_read_vendas_ignora_linhas_curtas(spark: SparkSession, tmp_path):
    #Garante que linhas fora do padrão de 31 caracteres são descartadas
    txt_file = tmp_path / "vendas.txt"
    linha_valida = "V000100001P00010001500020230401\n"
    linha_curta = "V000200001P00\n"
    
    txt_file.write_text(linha_valida + linha_curta, encoding="utf-8")
    
    df = read_vendas(spark, str(txt_file))
    resultados = df.collect()
    
    # Apenas a linha com 31 caracteres deve ser mantida
    assert len(resultados) == 1
    assert resultados[0]["venda_id"] == "V0001"

def test_read_vendas_arquivo_nao_encontrado(spark: SparkSession):
    #Garante que a função gera FileNotFoundError se o TXT não existir
    caminho_invalido = "caminho/falso/vendas.txt"
    with pytest.raises(FileNotFoundError, match="Arquivo de vendas não encontrado"):
        read_vendas(spark, caminho_invalido)