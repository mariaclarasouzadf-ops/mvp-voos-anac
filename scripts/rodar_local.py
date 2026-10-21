"""Executa os notebooks do projeto fora do Databricks, com PySpark local (apenas para testes).

Uso:
    python scripts/rodar_local.py notebooks <pasta_com_csvs> 01_coleta_bronze.py 02_transformacao_silver.py \
        03_modelagem_gold.py 04_qualidade_dados.py 05_analise.py

Todos os notebooks precisam rodar na MESMA chamada: o catálogo local fica em memória.
Requisitos: Java 17+, pip install pyspark pandas pyarrow matplotlib
"""
import os, sys, re
os.environ["MVP_MODO_LOCAL"]="1"; os.environ["MVP_CAMINHO_RAW"]=sys.argv[2]
from pyspark.sql import SparkSession
spark=(SparkSession.builder.master("local[2]").config("spark.sql.ansi.enabled","true")
       .config("spark.sql.session.timeZone","America/Sao_Paulo").config("spark.ui.enabled","false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.show=lambda: None
def display(df):
    try: df.show(40, truncate=60)
    except AttributeError: print(df.to_string() if hasattr(df,"to_string") else df)
nbdir=sys.argv[1]
def run(path, ns):
    src=open(path,encoding="utf-8").read()
    for cell in src.split("# COMMAND ----------"):
        lines=[l for l in cell.strip().splitlines() if l.strip()!="# Databricks notebook source"]
        if not lines: continue
        if lines[0].startswith("# MAGIC %run"):
            run(os.path.join(nbdir, lines[0].split("./")[1].strip()+".py"), ns); continue
        if lines[0].startswith("# MAGIC"): continue
        exec(compile("\n".join(lines), path, "exec"), ns)
for nb in sys.argv[3:]:
    print("\n========", nb); ns={"spark":spark,"display":display}
    run(os.path.join(nbdir, nb), ns)
