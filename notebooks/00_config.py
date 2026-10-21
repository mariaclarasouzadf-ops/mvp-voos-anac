# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuração do projeto
# MAGIC
# MAGIC Este notebook é executado no início de todos os outros (via `%run ./00_config`).
# MAGIC Ele centraliza **onde** os dados ficam na nuvem e as **constantes de negócio** do projeto,
# MAGIC para que nenhuma regra fique "escondida" no meio do código.
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Catálogo (Unity Catalog) | `workspace` (catálogo padrão do Databricks Free Edition) |
# MAGIC | Schema | `mvp_voos` |
# MAGIC | Volume com os arquivos brutos | `/Volumes/workspace/mvp_voos/raw` |

# COMMAND ----------

import os

# --- Localização na nuvem -------------------------------------------------
CATALOGO = "workspace"
SCHEMA = "mvp_voos"
VOLUME = "raw"
CAMINHO_RAW = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"


def t(nome: str) -> str:
    """Nome completo de uma tabela do projeto (catalogo.schema.tabela)."""
    return f"{CATALOGO}.{SCHEMA}.{nome}"


# --- Constantes de negócio (usadas nas regras de transformação) -----------
LIMIAR_ATRASO_MIN = 30          # ANAC considera atrasado o voo com mais de 30 min de diferença
ATRASO_MIN_PLAUSIVEL = -720     # adiantamento máximo aceito: 12 h
ATRASO_MAX_PLAUSIVEL = 1440     # atraso máximo aceito: 24 h
ASSENTOS_MAX_PLAUSIVEL = 600    # nenhuma aeronave comercial tem mais assentos que isso
REGEX_ICAO = r"^[A-Z0-9]{4}$"   # código ICAO de aeroporto: 4 caracteres
REGEX_ICAO_BRASIL = r"^S[BDIJNSW][A-Z0-9]{2}$"  # prefixos ICAO usados no Brasil
AEROPORTO_FOCO = "SBBR"         # Brasília (BSB)

# --- Modo de teste local (opcional) ---------------------------------------
# Permite rodar os mesmos notebooks fora do Databricks, com PySpark local.
MODO_LOCAL = os.environ.get("MVP_MODO_LOCAL") == "1"
if MODO_LOCAL:
    CAMINHO_RAW = os.environ["MVP_CAMINHO_RAW"]

    def t(nome: str) -> str:  # noqa: F811
        return f"{SCHEMA}.{nome}"

# COMMAND ----------

# Cria o schema e o volume (comandos idempotentes: podem rodar várias vezes sem erro).
if MODO_LOCAL:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA}")
else:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA} "
              "COMMENT 'MVP UnB - pipeline de pontualidade de voos (ANAC/VRA)'")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME} "
              "COMMENT 'Arquivos CSV brutos do VRA baixados do portal da ANAC'")

print(f"Configuração carregada. Arquivos brutos em: {CAMINHO_RAW}")
