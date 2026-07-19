"""
Módulo de Qualidade de Dados (Data Quality)

"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

def _formatar_alerta(nome_df: str, coluna: str, erro_count: int, total: int, tipo: str) -> str:
    #Função auxiliar para calcular a porcentagem e formatar a mensagem
    porcentagem = (erro_count / total) * 100 if total > 0 else 0
    return f"⚠️ Alerta DQ ({tipo} - {nome_df}.{coluna}): {erro_count} registros falharam ({porcentagem:.2f}% do total)."

def check_unicidade(df: DataFrame, nome_df: str, coluna_chave: str) -> None:
    # Valida a quantidade de registros duplicados na coluna chave
    total = df.count()
    distintos = df.select(coluna_chave).distinct().count()
    duplicados = total - distintos
    
    if duplicados > 0:
        logger.warning(_formatar_alerta(nome_df, coluna_chave, duplicados, total, "Unicidade"))
    else:
        logger.info(f"✅ Sucesso DQ (Unicidade - {nome_df}): Nenhum duplicado em '{coluna_chave}'.")

def check_completude(df: DataFrame, nome_df: str, colunas: list[str]) -> None:
    # Valida a quantidade de valores nulos nas colunas obrigatórias
    total = df.count()
    for col in colunas:
        nulos = df.filter(F.col(col).isNull()).count()
        if nulos > 0:
            logger.warning(_formatar_alerta(nome_df, col, nulos, total, "Completude"))
        else:
            logger.info(f"✅ Sucesso DQ (Completude - {nome_df}): A coluna '{col}' está 100% preenchida.")

def check_integridade_referencial(df_fato: DataFrame, df_dimensao: DataFrame, nome_fato: str, nome_dim: str, coluna_chave: str) -> None:
    # Valida quantas vendas não possuem um cliente correspondente no cadastro
    total = df_fato.count()
    orfaos = df_fato.join(df_dimensao, on=coluna_chave, how="left_anti").count()
    
    if orfaos > 0:
        logger.warning(f"⚠️ Alerta DQ (Integridade - {nome_fato} vs {nome_dim}): {orfaos} vendas associadas a clientes inexistentes ({(orfaos/total)*100:.2f}%).")
    else:
        logger.info(f"✅ Sucesso DQ (Integridade - {nome_fato} vs {nome_dim}): 100% das vendas possuem clientes cadastrados.")