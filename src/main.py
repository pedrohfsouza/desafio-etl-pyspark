"""
Módulo main do pipeline

"""

import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession

from .readers import read_clientes, read_vendas
from .transformers import (
    join_vendas_clientes,
    resumo_por_cliente,
    balanco_por_produto,
)
from .writers import write_parquet

def _configurar_log(nivel: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, nivel.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

def _get_spark(app_name: str = "desafio-etl-pyspark") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL PySpark - Clientes x Vendas",
    )
    parser.add_argument("--clientes", required=True, help="Caminho para clientes.csv")
    parser.add_argument("--vendas", required=True, help="Caminho para vendas.txt")
    parser.add_argument("--output", required=True, help="Diretório base de saída")
    parser.add_argument("--log-level", default="INFO")
    
    return parser.parse_args(argv)

def run(clientes_path: str, vendas_path: str, output_path: str, spark: SparkSession | None = None) -> int:
    #Executa o pipeline. Retorna 0 em sucesso, != 0 em falha
    logger = logging.getLogger("main")
    
    _stop_spark = spark is None
    if spark is None:
        spark = _get_spark()
        
    try:
        # Extract
        df_clientes = read_clientes(spark, clientes_path)
        df_vendas = read_vendas(spark, vendas_path)

        # Transform
        df_join = join_vendas_clientes(df_vendas, df_clientes)
        df_resumo = resumo_por_cliente(df_join)
        df_balanco = balanco_por_produto(df_vendas)

        # Load
        out_resumo = os.path.join(output_path, "resumo_clientes")
        out_balanco = os.path.join(output_path, "balanco_produtos")

        # Outputs particionados por data_venda
        write_parquet(df_resumo, out_resumo)
        write_parquet(df_balanco, out_balanco)

        logger.info("Pipeline finalizado com sucesso.")
        return 0

    except FileNotFoundError as e:
        logger.error("Arquivo de entrada não encontrado: %s", e)
        return 2
    except Exception as e:
        logger.exception("Falha inesperada no pipeline: %s", e)
        return 1
    finally:
        if _stop_spark:
            spark.stop()

def main(argv=None) -> int:
    args = parse_args(argv)
    _configurar_log(args.log_level)
    return run(
        clientes_path=args.clientes,
        vendas_path=args.vendas,
        output_path=args.output,
    )

if __name__ == "__main__":
    sys.exit(main())