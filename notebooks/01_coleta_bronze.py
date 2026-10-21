# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Coleta → camada Bronze
# MAGIC
# MAGIC **O que este notebook faz:** lê os arquivos CSV mensais do VRA que foram enviados para o Volume
# MAGIC `/Volumes/workspace/mvp_voos/raw` e grava tudo, **sem alterar nenhum valor**, na tabela Delta
# MAGIC `bronze_vra`.
# MAGIC
# MAGIC **Por que uma camada Bronze?** Ela é a cópia fiel da origem. Se uma regra de transformação
# MAGIC estiver errada, dá para refazer tudo a partir daqui, sem baixar os dados de novo.
# MAGIC
# MAGIC **Decisões desta etapa**
# MAGIC 1. Todas as colunas são lidas como **texto**. A tipagem (datas, números) fica para a Silver,
# MAGIC    onde cada conversão é documentada.
# MAGIC 2. O leitor **detecta sozinho** a codificação (UTF-8 ou Latin-1), o separador (`;` ou `,`) e
# MAGIC    se existe uma linha extra antes do cabeçalho. Arquivos antigos da ANAC começam com
# MAGIC    "Atualizado em: ...".
# MAGIC 3. Os nomes das colunas são **padronizados** (sem acento, minúsculos, `snake_case`) e mapeados para
# MAGIC    nomes canônicos. A ANAC já mudou os nomes das colunas entre anos; o mapeamento absorve isso.
# MAGIC 4. Cada linha recebe a **linhagem**: o nome do arquivo de origem e o instante da ingestão.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import re
import unicodedata

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mapeamento de colunas (origem → nome canônico)

# COMMAND ----------

# nome original normalizado  ->  nome canônico usado no projeto
ALIASES = {
    "sigla_icao_empresa_aerea": "cod_empresa",
    "icao_empresa_aerea": "cod_empresa",
    "empresa_aerea": "nome_empresa",
    "numero_voo": "numero_voo",
    "numero_do_voo": "numero_voo",
    "codigo_di": "codigo_di",
    "codigo_autorizacao_di": "codigo_di",
    "codigo_autorizacao": "codigo_di",
    "codigo_tipo_linha": "tipo_linha",
    "modelo_equipamento": "modelo_equipamento",
    "numero_de_assentos": "assentos",
    "numeros_de_assentos": "assentos",
    "numero_assentos": "assentos",
    "sigla_icao_aeroporto_origem": "cod_origem",
    "icao_aerodromo_origem": "cod_origem",
    "sigla_icao_aerodromo_origem": "cod_origem",
    "descricao_aeroporto_origem": "nome_origem",
    "partida_prevista": "partida_prevista",
    "partida_real": "partida_real",
    "sigla_icao_aeroporto_destino": "cod_destino",
    "icao_aerodromo_destino": "cod_destino",
    "sigla_icao_aerodromo_destino": "cod_destino",
    "descricao_aeroporto_destino": "nome_destino",
    "chegada_prevista": "chegada_prevista",
    "chegada_real": "chegada_real",
    "situacao_voo": "situacao_voo",
    "justificativa": "codigo_justificativa",
    "codigo_justificativa": "codigo_justificativa",
    "referencia": "referencia",
    "situacao_partida": "situacao_partida",
    "situacao_chegada": "situacao_chegada",
    "codeshare": "codeshare",
}

COLUNAS_CANONICAS = [
    "cod_empresa", "nome_empresa", "numero_voo", "codigo_di", "tipo_linha",
    "modelo_equipamento", "assentos", "cod_origem", "nome_origem",
    "partida_prevista", "partida_real", "cod_destino", "nome_destino",
    "chegada_prevista", "chegada_real", "situacao_voo", "codigo_justificativa",
    "referencia", "situacao_partida", "situacao_chegada", "codeshare",
]


def normalizar(texto: str) -> str:
    """'Sigla ICAO Empresa Aérea' -> 'sigla_icao_empresa_aerea'."""
    s = unicodedata.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-zA-Z]+", "_", s).strip("_").lower()


def detectar_formato(caminho: str):
    """Descobre codificação, linha do cabeçalho e separador de um CSV do VRA."""
    with open(caminho, "rb") as f:
        conteudo = f.read()
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            texto = conteudo.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    for i, linha in enumerate(texto.splitlines()[:10]):
        if "partida_prevista" in normalizar(linha):
            separador = ";" if linha.count(";") >= linha.count(",") else ","
            return encoding, i, separador, len(conteudo)
    raise ValueError(f"Cabeçalho do VRA não encontrado em {caminho}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos arquivos e gravação da tabela Bronze

# COMMAND ----------

arquivos = sorted(f for f in os.listdir(CAMINHO_RAW) if f.lower().endswith(".csv"))
assert arquivos, f"Nenhum CSV encontrado em {CAMINHO_RAW}. Faça o upload dos arquivos do VRA."
print(f"{len(arquivos)} arquivo(s) encontrado(s):", arquivos)

schema_bronze = T.StructType(
    [T.StructField(c, T.StringType(), True) for c in COLUNAS_CANONICAS + ["arquivo_origem"]]
)

log_coleta = []
for n, nome_arquivo in enumerate(arquivos):
    caminho = f"{CAMINHO_RAW}/{nome_arquivo}"
    encoding, linha_cabecalho, separador, tamanho = detectar_formato(caminho)

    pdf = pd.read_csv(caminho, sep=separador, skiprows=linha_cabecalho, encoding=encoding,
                      dtype=str, keep_default_na=False, na_values=[""])
    originais = list(pdf.columns)
    pdf.columns = [ALIASES.get(normalizar(c), normalizar(c)) for c in originais]
    pdf = pdf.loc[:, ~pdf.columns.duplicated()]
    ignoradas = [o for o, c in zip(originais, pdf.columns) if c not in COLUNAS_CANONICAS]
    for c in COLUNAS_CANONICAS:
        if c not in pdf.columns:
            pdf[c] = None
    pdf["arquivo_origem"] = nome_arquivo
    pdf = pdf[COLUNAS_CANONICAS + ["arquivo_origem"]].astype(object)
    pdf = pdf.where(pdf.notna(), None)

    sdf = spark.createDataFrame(pdf, schema=schema_bronze).withColumn("data_ingestao", F.current_timestamp())
    (sdf.write.format("delta" if not MODO_LOCAL else "parquet")
        .mode("overwrite" if n == 0 else "append")
        .option("overwriteSchema", "true")
        .saveAsTable(t("bronze_vra")))

    log_coleta.append({
        "arquivo": nome_arquivo, "linhas": len(pdf), "tamanho_mb": round(tamanho / 1e6, 2),
        "encoding": encoding, "separador": separador, "linhas_puladas": linha_cabecalho,
        "colunas_ignoradas": ", ".join(ignoradas) or "-",
    })
    print(f"✔ {nome_arquivo}: {len(pdf):,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro da coleta (reprodutibilidade)
# MAGIC Volume obtido por arquivo, formato original e data da ingestão. Esta tabela também é salva
# MAGIC na nuvem (`log_coleta`) e serve como evidência da etapa de Coleta.

# COMMAND ----------

log_df = (spark.createDataFrame(pd.DataFrame(log_coleta))
          .withColumn("data_ingestao", F.current_timestamp()))
(log_df.write.format("delta" if not MODO_LOCAL else "parquet").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(t("log_coleta")))
display(log_df)

# COMMAND ----------

total = spark.table(t("bronze_vra")).count()
print(f"Tabela {t('bronze_vra')} gravada com {total:,} linhas.")
display(spark.sql(f"SELECT arquivo_origem, COUNT(*) AS linhas FROM {t('bronze_vra')} "
                  "GROUP BY arquivo_origem ORDER BY arquivo_origem"))
