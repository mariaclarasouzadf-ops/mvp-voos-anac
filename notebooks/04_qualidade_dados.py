# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Análise de qualidade dos dados
# MAGIC
# MAGIC **O que este notebook faz:** examina **cada atributo** antes de responder às perguntas de negócio,
# MAGIC nas dimensões pedidas pelo trabalho:
# MAGIC
# MAGIC | Dimensão | Pergunta | Onde é verificada |
# MAGIC |---|---|---|
# MAGIC | Completude | Há nulos? O que significam? | Perfil por atributo (parte 1) |
# MAGIC | Unicidade | Há voos duplicados? | Teste Q1 |
# MAGIC | Consistência | Os atributos se contradizem? | Testes Q2–Q5 |
# MAGIC | Conformidade | Os valores respeitam o domínio declarado no catálogo? | Testes Q6–Q9 e domínios da parte 1 |
# MAGIC | Acurácia | Há valores implausíveis? | Testes Q10–Q11 |
# MAGIC | Atualidade | Qual o período coberto? | Parte 3 |
# MAGIC
# MAGIC Cada problema encontrado tem um **tratamento** indicado, ligado às regras R1–R11 da Silver.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

bronze = spark.table(t("bronze_vra"))
silver = spark.table(t("silver_voos"))
FORMATO = "delta" if not MODO_LOCAL else "parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1 · Perfil de cada atributo (Silver)
# MAGIC Para cada coluna: tipo, % de nulos, valores distintos, mínimo e máximo (numéricos e datas)
# MAGIC e os valores mais frequentes (categóricos).

# COMMAND ----------

ATRIBUTOS = [c for c in silver.columns if not c.endswith("_txt") and c not in ("id_voo",)]
total = silver.count()

aggs = []
for c in ATRIBUTOS:
    aggs += [F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__nulos"),
             F.approx_count_distinct(c).alias(f"{c}__distintos")]
    tipo = silver.schema[c].dataType
    if isinstance(tipo, (T.NumericType, T.TimestampType, T.DateType)):
        aggs += [F.min(c).cast("string").alias(f"{c}__min"), F.max(c).cast("string").alias(f"{c}__max")]
resumo = silver.agg(*aggs).first().asDict()

linhas = []
for c in ATRIBUTOS:
    tipo = silver.schema[c].dataType.simpleString()
    distintos = resumo[f"{c}__distintos"]
    top = "-"
    if tipo in ("string", "boolean") and distintos <= 40:
        freq = (silver.groupBy(c).count().orderBy(F.desc("count")).limit(8).collect())
        top = "; ".join(f"{r[c]} ({100 * r['count'] / total:.1f}%)" for r in freq)
    linhas.append({
        "atributo": c, "tipo": tipo,
        "pct_nulos": round(100 * resumo[f"{c}__nulos"] / total, 2),
        "distintos_aprox": distintos,
        "minimo": resumo.get(f"{c}__min", "-"), "maximo": resumo.get(f"{c}__max", "-"),
        "valores_mais_frequentes": top,
    })

perfil = pd.DataFrame(linhas)
perfil_sdf = spark.createDataFrame(perfil.astype(str))
perfil_sdf.write.format(FORMATO).mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("qualidade_perfil"))
display(perfil_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2 · Testes de qualidade
# MAGIC Cada teste conta quantos registros violam uma expectativa. **0** significa "nenhum problema encontrado",
# MAGIC e isso também precisa ser demonstrado, como pede o enunciado.

# COMMAND ----------

duplicatas_bronze = bronze.count() - silver.count()

TESTES = [
    # (id, dimensão, descrição, expressão SQL que identifica o problema, tratamento)
    ("Q2", "Consistência", "Voo CANCELADO com partida real preenchida",
     "cancelado AND partida_real IS NOT NULL", "Mantido como cancelado; partida real ignorada (R5)"),
    ("Q3", "Consistência", "Voo REALIZADO sem partida real",
     "realizado AND partida_real IS NULL", "Atraso fica nulo e o voo sai das médias de atraso (R5)"),
    ("Q4", "Consistência", "Chegada real anterior à partida real",
     "chegada_real < partida_real", "Sinalizado; não afeta o atraso de partida usado nas perguntas"),
    ("Q5", "Consistência", "Origem igual ao destino",
     "cod_origem = cod_destino", "Voo de retorno/alternado; permanece, mas é minoria"),
    ("Q6", "Conformidade", "Código ICAO de origem/destino fora do padrão de 4 caracteres",
     f"NOT (cod_origem RLIKE '{REGEX_ICAO}') OR NOT (cod_destino RLIKE '{REGEX_ICAO}')", "Fica fora da análise doméstica (R8)"),
    ("Q7", "Conformidade", "Situação do voo fora de {REALIZADO, CANCELADO}",
     "situacao_voo IS NULL OR situacao_voo NOT IN ('REALIZADO','CANCELADO')", "Não entra em taxas de atraso nem de cancelamento"),
    ("Q8", "Conformidade", "Data/hora presente no arquivo mas não convertida",
     "(partida_prevista IS NULL AND partida_prevista_txt IS NOT NULL) OR (partida_real IS NULL AND partida_real_txt IS NOT NULL)",
     "Formato novo deve ser incluído em FORMATOS_DATA (R3)"),
    ("Q9", "Conformidade", f"Assentos fora de [0, {ASSENTOS_MAX_PLAUSIVEL}]",
     f"assentos < 0 OR assentos > {ASSENTOS_MAX_PLAUSIVEL}", "Atributo não usado nas perguntas"),
    ("Q10", "Acurácia", "Atraso de partida implausível (< -12 h ou > 24 h)",
     "atraso_valido = false", "Atraso anulado (R6)"),
    ("Q11", "Completude", "Voo sem horário previsto (extra/fretado)",
     "NOT voo_previsto", "Excluído das métricas de pontualidade (R9)"),
]

resultados = [("Q1", "Unicidade", "Linhas duplicadas removidas na Silver", duplicatas_bronze,
               round(100 * duplicatas_bronze / max(bronze.count(), 1), 3), "Deduplicação (R10)")]
contagens = silver.agg(*[F.sum(F.coalesce(F.expr(expr), F.lit(False)).cast("int")).alias(i)
                         for i, _, _, expr, _ in TESTES]).first().asDict()
for i, dim, desc, _, trat in TESTES:
    n = contagens[i] or 0
    resultados.append((i, dim, desc, n, round(100 * n / total, 3), trat))

# Empresa com mais de um nome (consistência de cadastro)
multi_nome = silver.groupBy("cod_empresa").agg(F.countDistinct("nome_empresa").alias("n")).filter("n > 1").count()
resultados.append(("Q12", "Consistência", "Código de empresa com mais de um nome", multi_nome, None,
                   "Usa o nome mais frequente na dim_empresa"))

checks = spark.createDataFrame(resultados, "teste STRING, dimensao STRING, descricao STRING, "
                                           "registros_afetados LONG, pct DOUBLE, tratamento STRING")
checks.write.format(FORMATO).mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("qualidade_testes"))
display(checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 3 · Atualidade e cobertura
# MAGIC Período coberto e volume por mês. Um mês com volume muito menor que os demais indica arquivo incompleto.

# COMMAND ----------

display(spark.sql(f"""
SELECT date_format(data_voo, 'yyyy-MM') AS mes, COUNT(*) AS voos,
       SUM(CAST(domestico AND voo_previsto AS INT)) AS voos_base_analise
FROM {t('silver_voos')} GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 4 · Validação cruzada: classificação da ANAC × regra de aeroportos
# MAGIC A ANAC classifica cada voo por **tipo de linha** (N = doméstica mista, C = doméstica cargueira,
# MAGIC I = internacional mista, G = internacional cargueira). Nós também classificamos pelo prefixo ICAO
# MAGIC dos aeroportos. Se as duas fontes concordarem, as linhas N e C devem ter **100%** dos voos entre
# MAGIC aeroportos brasileiros. Isso testa a consistência da regra R8 com uma fonte independente.

# COMMAND ----------

display(spark.sql(f"""
SELECT COALESCE(tipo_linha, '(nulo)') AS tipo_linha, COUNT(*) AS voos,
       ROUND(100 * AVG(CAST(cod_origem RLIKE '{REGEX_ICAO_BRASIL}' AND cod_destino RLIKE '{REGEX_ICAO_BRASIL}' AS INT)), 2)
         AS pct_entre_aeroportos_brasileiros
FROM {t('silver_voos')}
GROUP BY 1 ORDER BY voos DESC
"""))
