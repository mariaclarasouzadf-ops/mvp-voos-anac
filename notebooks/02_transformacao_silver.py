# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Transformação → camada Silver
# MAGIC
# MAGIC **O que este notebook faz:** pega a Bronze (tudo texto, cópia fiel) e produz a tabela
# MAGIC `silver_voos`: tipada, padronizada, sem duplicatas e com as métricas de atraso calculadas.
# MAGIC
# MAGIC ## Regras de negócio aplicadas (explícitas, porque mudam a interpretação dos resultados)
# MAGIC
# MAGIC | # | Regra | Motivo |
# MAGIC |---|---|---|
# MAGIC | R1 | Remover espaços nas pontas e converter texto vazio, `N/A` e `null` em nulo | Os arquivos da ANAC escrevem "ausente" de três jeitos; nenhum deles pode virar valor real |
# MAGIC | R2 | Códigos (empresa, aeroportos, situação) em MAIÚSCULAS | Padronizar chaves para os *joins* do modelo estrela |
# MAGIC | R3 | Datas/horas convertidas aceitando `dd/MM/yyyy HH:mm[:ss]` e `yyyy-MM-dd HH:mm[:ss]`, descartando frações de segundo (`.000000000`) | A ANAC já publicou os dois formatos e ~7% das linhas de 2025 têm frações de segundo; o que não converter vira nulo e é contado no teste Q8 |
# MAGIC | R4 | `atraso_partida_min` = partida real − partida prevista, em minutos (idem para a chegada) | Métrica central do trabalho; valores negativos são adiantamentos |
# MAGIC | R5 | O atraso só é calculado para voos **REALIZADOS** com os dois horários preenchidos | Voo cancelado não tem atraso |
# MAGIC | R6 | Atraso fora de [−12 h, +24 h] é considerado implausível e anulado (`atraso_valido = false`) | Erro de digitação na origem (ex.: data trocada) distorceria médias |
# MAGIC | R7 | Atrasado = atraso de partida **> 30 min** | Critério de atraso usado pela ANAC |
# MAGIC | R8 | Voo doméstico de passageiros = tipo de linha **N** (doméstica mista) **e** origem e destino com prefixo ICAO brasileiro (`SB`, `SD`, `SI`, `SJ`, `SN`, `SS`, `SW`) | O problema trata do passageiro na malha doméstica; exclui cargueiros (C, G) e internacionais (I) |
# MAGIC | R9 | Voo sem horário previsto (extra, fretado, alternado) é marcado com `voo_previsto = false` | Sem horário previsto não existe atraso a medir |
# MAGIC | R10 | Deduplicação pela chave empresa + nº do voo + origem + destino + horário de referência; mantém a ingestão mais recente | O mesmo voo pode aparecer em dois arquivos (republicação da ANAC) |
# MAGIC | R11 | Faixa horária pela hora **prevista** de partida: Madrugada (0–5h), Manhã (6–11h), Tarde (12–17h), Noite (18–23h) | Base da hipótese do efeito cascata |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

bronze = spark.table(t("bronze_vra"))
funil = [("1. Linhas na Bronze", bronze.count())]

# COMMAND ----------

# MAGIC %md
# MAGIC ## R1, R2 e R3: limpeza, padronização e tipagem

# COMMAND ----------

FORMATOS_DATA = ["dd/MM/yyyy HH:mm", "dd/MM/yyyy HH:mm:ss",
                 "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM-dd'T'HH:mm:ss"]
COLUNAS_DATA = ["partida_prevista", "partida_real", "chegada_prevista", "chegada_real"]
COLUNAS_CODIGO = ["cod_empresa", "cod_origem", "cod_destino", "situacao_voo", "tipo_linha",
                  "codigo_justificativa", "situacao_partida", "situacao_chegada"]


def limpar(coluna):
    """R1: trim e texto vazio -> nulo."""
    c = F.trim(F.col(coluna))
    return F.when((c == "") | F.upper(c).isin("N/A", "NULL"), None).otherwise(c)


def para_timestamp(coluna):
    """R3: tenta cada formato conhecido; o primeiro que funcionar vence."""
    sem_fracao = F.regexp_replace(F.col(coluna), r"\.\d+$", "")
    return F.coalesce(*[F.try_to_timestamp(sem_fracao, F.lit(fmt)) for fmt in FORMATOS_DATA])


df = bronze
for c in bronze.columns:
    if c not in ("data_ingestao",):
        df = df.withColumn(c, limpar(c))
for c in COLUNAS_CODIGO:
    df = df.withColumn(c, F.upper(F.col(c)))
for c in COLUNAS_DATA:
    df = df.withColumn(c + "_txt", F.col(c)).withColumn(c, para_timestamp(c))

df = df.withColumn("assentos", F.expr("try_cast(assentos AS INT)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## R4 a R9 e R11: métricas e indicadores derivados

# COMMAND ----------

def minutos(fim, inicio):
    return (F.unix_timestamp(F.col(fim)) - F.unix_timestamp(F.col(inicio))) / 60.0


realizado = F.col("situacao_voo") == "REALIZADO"
plausivel = lambda c: F.col(c).between(ATRASO_MIN_PLAUSIVEL, ATRASO_MAX_PLAUSIVEL)

df = (df
      .withColumn("cancelado", F.col("situacao_voo") == "CANCELADO")
      .withColumn("realizado", F.coalesce(realizado, F.lit(False)))
      # R4 + R5
      .withColumn("atraso_partida_min",
                  F.when(F.col("realizado"), minutos("partida_real", "partida_prevista")))
      .withColumn("atraso_chegada_min",
                  F.when(F.col("realizado"), minutos("chegada_real", "chegada_prevista")))
      # R6
      .withColumn("atraso_valido",
                  F.when(F.col("atraso_partida_min").isNull(), None)
                   .otherwise(plausivel("atraso_partida_min")))
      .withColumn("atraso_partida_min",
                  F.when(plausivel("atraso_partida_min"), F.round("atraso_partida_min", 0)))
      .withColumn("atraso_chegada_min",
                  F.when(plausivel("atraso_chegada_min"), F.round("atraso_chegada_min", 0)))
      # R7
      .withColumn("atrasado_30", F.col("atraso_partida_min") > LIMIAR_ATRASO_MIN)
      # R8
      .withColumn("domestico", F.coalesce((F.col("tipo_linha") == "N")
                                          & F.col("cod_origem").rlike(REGEX_ICAO_BRASIL)
                                          & F.col("cod_destino").rlike(REGEX_ICAO_BRASIL),
                                          F.lit(False)))
      # R9
      .withColumn("voo_previsto", F.col("partida_prevista").isNotNull())
      # chaves de tempo
      .withColumn("horario_referencia", F.coalesce("partida_prevista", "partida_real"))
      .withColumn("data_voo", F.to_date("horario_referencia"))
      .withColumn("hora_prevista", F.hour("partida_prevista"))
      # R11
      .withColumn("faixa_horaria",
                  F.when(F.col("hora_prevista") < 6, "1-Madrugada")
                   .when(F.col("hora_prevista") < 12, "2-Manhã")
                   .when(F.col("hora_prevista") < 18, "3-Tarde")
                   .when(F.col("hora_prevista").isNotNull(), "4-Noite")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## R10: deduplicação

# COMMAND ----------

chave = ["cod_empresa", "numero_voo", "cod_origem", "cod_destino", "horario_referencia"]
janela = Window.partitionBy(*chave).orderBy(F.col("data_ingestao").desc(), F.col("arquivo_origem").desc())
df = (df.withColumn("_ordem", F.row_number().over(janela))
        .filter("_ordem = 1").drop("_ordem"))

# chave técnica do voo (hash da chave natural) — usada como identificador na tabela fato
df = df.withColumn("id_voo", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("~"))
                                                    for c in chave]), 256))

# COMMAND ----------

(df.write.format("delta" if not MODO_LOCAL else "parquet").mode("overwrite")
   .option("overwriteSchema", "true").saveAsTable(t("silver_voos")))

silver = spark.table(t("silver_voos"))
funil += [
    ("2. Após deduplicação (R10)", silver.count()),
    ("3. Somente domésticos de passageiros (R8)", silver.filter("domestico").count()),
    ("4. Domésticos com horário previsto (R9) = base de análise", silver.filter("domestico AND voo_previsto").count()),
    ("5. ...dos quais realizados", silver.filter("domestico AND voo_previsto AND realizado").count()),
    ("6. ...dos quais cancelados", silver.filter("domestico AND voo_previsto AND cancelado").count()),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funil da transformação
# MAGIC Mostra quantas linhas cada regra afeta. É a forma mais simples de explicar o ETL na apresentação.

# COMMAND ----------

funil_df = spark.createDataFrame(funil, "etapa STRING, linhas LONG")
(funil_df.write.format("delta" if not MODO_LOCAL else "parquet").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(t("log_funil_transformacao")))
display(funil_df)
