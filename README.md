# Desafio Técnico - ETL com PySpark

Pipeline de ETL em **PySpark** que integra dados de clientes (CSV) com um arquivo posicional de vendas (TXT), gerando dois relatórios em **Parquet**: **'resumo_clientes'** e **'balanco_produtos'**

## Estrutura do projeto

```
desafio-etl-pyspark/
├── data/     
│    ├── input/                     # arquivos csv e txt de entrada
│    ├── output/                    # gerado em runtime
├── src/
│   ├── __init__.py                 
│   ├── config.py                   # Schemas e layout posicional
│   ├── readers.py                  # Leitura dos arquivos de input
│   ├── transformers.py             # Join + agregações (transformações)
│   ├── writers.py                  # Escrita Parquet (snappy) + particionamento
│   ├── data_quality.py             # Módulo de observability (Qualidade de Dados)
│   └── main.py                     # Entrypoint
├── tests/
│   ├── __init__.py
│   ├── conftest.py                 # SparkSession compartilhado
│   ├── test_readers.py             # Testes da leitura
│   ├── test_transformers.py        # Testes das transformações
│   └── test_pipeline.py            # Testes de integração
├── requirements.txt
└── README.md
```

## Tecnologias

- **Linguagem**: Python
- **Processamento**: PySpark (necessário Java instalado para execução local)
- **Testes**: Pytest

## Como instalar

```bash
    # Clone o repositório
    git clone https://github.com/pedrohfsouza/desafio-etl-pyspark.git
    cd desafio-etl-pyspark

    # Instale as dependências
    pip install -r requirements.txt
```

## Como executar o pipeline

O pipeline aceita caminhos customizados para leitura e escrita.
Com os arquivos de entrada em `data/input/`:

```bash
python -m src.main \
    --clientes data/input/clientes.csv \
    --vendas data/input/vendas.txt \
    --output data/output
```

## Como executar os testes automatizados

Os testes com **pytest** cobrem a leitura dos arquivos, agregações/transformações e integração end-to-end:

```bash
pytest tests/ -v
```

## Exemplos de Dados

### Entrada

- **clientes.csv**:
```csv
cliente_id,nome,data_nascimento
1,João Silva,1980-05-12
2,Maria Souza,1995-07-30
3,Carlos Andrade,1988-02-20
4,Ana Beatriz,1992-11-04
```

- **vendas.txt**:
```
0000100001000010012345020260403
0000200002000020000120020260403
0000300001000020005000020260403
0000400002000010008000020260410
0000500001000010001250020260415
0000600004000020000990020260418
0000700001000030001500020260420
0000800003000020002500020260422
```

Interpretação da 1ª linha: venda `00001`, cliente `00001`, produto `00001`, valor `1234.50`, data `2026-04-03`

### Saída

O pipeline exporta os dados no formato Parquet particionados por `data_venda`:

```
data/output/
├── resumo_clientes/
│   └── data_venda=2026-04-03/
│       └── part-00000-...snappy.parquet
└── balanco_produtos/
    └── data_venda=2023-04-03/
        └── part-00000-...snappy.parquet
```

### Data Observability

O módulo de Data Quality apura métricas de unicidade, completude e integridade. Caso existam inconsistências, o sistema gera logs com quantidade e percentual de registros afetados, garantindo visibilidade sem interromper o fluxo do pipeline.