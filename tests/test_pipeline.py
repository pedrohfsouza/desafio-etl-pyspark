"""Testes de integração do pipeline."""

import os
from pyspark.sql import SparkSession

from src.main import run

def test_pipeline_sucesso_escreve_parquets(spark: SparkSession, tmp_path):
    #Executa o pipeline inteiro e valida a criação dos particionamentos

    # Configuração dos caminhos temporários
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    
    clientes_path = input_dir / "clientes.csv"
    vendas_path = input_dir / "vendas.txt"
    
    clientes_path.write_text("cliente_id,nome\n1,Joao\n", encoding="utf-8")
    vendas_path.write_text("V000100001P00010001500020230401\n", encoding="utf-8")
    
    codigo_retorno = run(
        clientes_path=str(clientes_path),
        vendas_path=str(vendas_path),
        output_path=str(output_dir),
        spark=spark
    )
    
    # Valida que o pipeline não teve erros
    assert codigo_retorno == 0
    
    out_resumo = output_dir / "resumo_clientes"
    out_balanco = output_dir / "balanco_produtos"
    
    # Valida se os diretórios finais foram criados
    assert out_resumo.exists()
    assert out_balanco.exists()
    
    # Valida se o PySpark criou a pasta da partição 'data_venda=2023-04-01'
    pastas_resumo = os.listdir(out_resumo)
    assert any("data_venda=2023-04-01" in pasta for pasta in pastas_resumo)

def test_pipeline_falha_arquivo_nao_encontrado(spark: SparkSession, tmp_path):
    # Garante que o pipeline devolve código 2 ao dar erro
    caminho_inexistente = str(tmp_path / "falso.txt")
    
    codigo_retorno = run(
        clientes_path=caminho_inexistente,
        vendas_path=caminho_inexistente,
        output_path=str(tmp_path / "output"),
        spark=spark
    )
    
    assert codigo_retorno == 2