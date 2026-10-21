# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Análise: respostas às perguntas de negócio
# MAGIC
# MAGIC **Base de análise:** voos **domésticos** com **horário previsto** (regras R8 e R9).
# MAGIC - **Taxa de atraso** = % dos voos realizados com partida mais de 30 min após o previsto (R7).
# MAGIC - **Taxa de cancelamento** = % dos voos programados que foram cancelados.
# MAGIC
# MAGIC Todas as consultas leem o **esquema estrela** (fato + dimensões), não a Silver.
# MAGIC Cada pergunta tem: consulta SQL → tabela → gráfico. A **discussão** de cada resposta fica no README.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import math

import matplotlib.pyplot as plt

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW base AS
SELECT f.*, e.nome_empresa, e.segmento, ao.nome_aeroporto AS nome_origem, ao.cidade AS cidade_origem, ao.uf AS uf_origem,
       ad.nome_aeroporto AS nome_destino, ad.cidade AS cidade_destino, ad.uf AS uf_destino,
       d.ano, d.mes, d.nome_mes, d.dia_semana_num, d.dia_semana_nome
FROM {t('fato_voo')} f
JOIN {t('dim_empresa')}   e  ON f.cod_empresa = e.cod_empresa
JOIN {t('dim_aeroporto')} ao ON f.cod_origem  = ao.cod_aeroporto
JOIN {t('dim_aeroporto')} ad ON f.cod_destino = ad.cod_aeroporto
JOIN {t('dim_data')}      d  ON f.data_id     = d.data_id
WHERE f.domestico AND f.voo_previsto
""")

# Métricas padronizadas, reutilizadas em todas as perguntas
METRICAS = """
  COUNT(*)                                                   AS voos_programados,
  ROUND(100 * AVG(CAST(cancelado AS INT)), 2)                AS pct_cancelados,
  ROUND(100 * AVG(CAST(atrasado_30 AS INT)), 2)              AS pct_atraso_30min,
  ROUND(AVG(atraso_partida_min), 1)                          AS atraso_medio_min,
  ROUND(percentile_approx(atraso_partida_min, 0.5), 0)       AS atraso_mediano_min
"""


def grafico_barras(pdf, x, y, titulo, rotulo_y, destaque=None, horizontal=False, linha_media=None):
    """Gráfico de barras simples e legível; `destaque` pinta uma categoria de outra cor."""
    cores = ["#c0392b" if destaque is not None and v == destaque else "#2e6da4" for v in pdf[x]]
    fig, ax = plt.subplots(figsize=(10, 5))
    if horizontal:
        ax.barh(pdf[x].astype(str), pdf[y], color=cores)
        ax.invert_yaxis()
        ax.set_xlabel(rotulo_y)
    else:
        ax.bar(pdf[x].astype(str), pdf[y], color=cores)
        ax.set_ylabel(rotulo_y)
        plt.xticks(rotation=45, ha="right")
    if linha_media is not None:
        (ax.axvline if horizontal else ax.axhline)(linha_media, color="gray", linestyle="--",
                                                   label=f"Média geral: {linha_media:.1f}")
        ax.legend()
    ax.set_title(titulo, fontsize=13, fontweight="bold")
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    plt.show()


geral = spark.sql(f"SELECT {METRICAS} FROM base").toPandas()
MEDIA_ATRASO = float(geral.pct_atraso_30min[0] or 0)
display(geral)

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 · Quais companhias aéreas atrasam e cancelam mais?
# MAGIC Apenas companhias **de passageiros** com pelo menos 1.000 voos no período, para evitar percentuais
# MAGIC instáveis calculados sobre poucos voos.

# COMMAND ----------

p1 = spark.sql(f"""
SELECT cod_empresa, nome_empresa, {METRICAS}
FROM base WHERE segmento = 'Passageiros'
GROUP BY cod_empresa, nome_empresa
HAVING COUNT(*) >= 1000
ORDER BY pct_atraso_30min DESC
""")
display(p1)
p1_pd = p1.toPandas()
grafico_barras(p1_pd, "nome_empresa", "pct_atraso_30min", "P1 · % de voos com atraso > 30 min por companhia",
               "% dos voos realizados", horizontal=True, linha_media=MEDIA_ATRASO)
grafico_barras(p1_pd.sort_values("pct_cancelados", ascending=False), "nome_empresa", "pct_cancelados",
               "P1 · % de voos cancelados por companhia", "% dos voos programados", horizontal=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 · Entre os 15 aeroportos mais movimentados, quais têm pior pontualidade de partida? Onde fica Brasília?

# COMMAND ----------

p2 = spark.sql(f"""
WITH top15 AS (
  SELECT cod_origem FROM base GROUP BY cod_origem ORDER BY COUNT(*) DESC LIMIT 15
)
SELECT b.cod_origem, b.nome_origem, {METRICAS},
       RANK() OVER (ORDER BY AVG(CAST(atrasado_30 AS INT)) DESC) AS ranking_atraso
FROM base b JOIN top15 USING (cod_origem)
GROUP BY b.cod_origem, b.nome_origem
ORDER BY pct_atraso_30min DESC
""")
display(p2)
p2_pd = p2.toPandas()
p2_pd["aeroporto"] = p2_pd.cod_origem + "\n" + p2_pd.nome_origem
grafico_barras(p2_pd, "aeroporto", "pct_atraso_30min",
               f"P2 · % de partidas com atraso > 30 min (destaque: {AEROPORTO_FOCO} = Brasília)",
               "% dos voos realizados", destaque=f"{AEROPORTO_FOCO}\nBrasília (JK)", linha_media=MEDIA_ATRASO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 · O risco de atraso cresce ao longo do dia? (hipótese do efeito cascata)
# MAGIC **Hipótese do MVP:** um atraso no primeiro voo da aeronave se propaga para os voos seguintes do dia.
# MAGIC Se isso for verdade, voos programados para a **manhã** atrasam significativamente menos do que os da **noite**.

# COMMAND ----------

p3_hora = spark.sql(f"SELECT hora_prevista, {METRICAS} FROM base GROUP BY hora_prevista ORDER BY hora_prevista")
display(p3_hora)
p3_pd = p3_hora.toPandas()
fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(p3_pd.hora_prevista, p3_pd.pct_atraso_30min, marker="o", color="#2e6da4", linewidth=2)
ax.set_xticks(range(0, 24))
ax.set_xlabel("Hora prevista de partida")
ax.set_ylabel("% dos voos realizados com atraso > 30 min")
ax.set_title("P3 · Taxa de atraso por hora prevista de partida", fontsize=13, fontweight="bold")
ax.grid(axis="y", alpha=0.3)
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

p3_faixa = spark.sql(f"""
SELECT faixa_horaria, {METRICAS},
       SUM(CAST(atrasado_30 AS INT)) AS voos_atrasados,
       COUNT(atrasado_30)            AS voos_com_atraso_medido
FROM base GROUP BY faixa_horaria ORDER BY faixa_horaria
""")
display(p3_faixa)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Teste estatístico da hipótese (Manhã × Noite)
# MAGIC Teste z para diferença de duas proporções. Se o valor-p ficar abaixo de 0,05, a diferença observada
# MAGIC dificilmente é obra do acaso.

# COMMAND ----------

f = p3_faixa.toPandas().set_index("faixa_horaria")
x1, n1 = f.loc["2-Manhã", "voos_atrasados"], f.loc["2-Manhã", "voos_com_atraso_medido"]
x2, n2 = f.loc["4-Noite", "voos_atrasados"], f.loc["4-Noite", "voos_com_atraso_medido"]
p1_, p2_ = x1 / n1, x2 / n2
p_comb = (x1 + x2) / (n1 + n2)
erro = math.sqrt(p_comb * (1 - p_comb) * (1 / n1 + 1 / n2))
z = (p2_ - p1_) / erro
valor_p = math.erfc(abs(z) / math.sqrt(2))  # bicaudal

print(f"Manhã: {100 * p1_:.2f}% de atraso  (n = {n1:,})")
print(f"Noite: {100 * p2_:.2f}% de atraso  (n = {n2:,})")
print(f"Diferença: {100 * (p2_ - p1_):+.2f} pontos percentuais | razão de risco: {p2_ / p1_:.2f}x")
print(f"z = {z:.2f} | valor-p = {valor_p:.2e}")
print("→ Hipótese SUSTENTADA pelos dados" if (valor_p < 0.05 and p2_ > p1_) else "→ Hipótese NÃO sustentada pelos dados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 · Existe sazonalidade? Em que meses e dias da semana a pontualidade piora?

# COMMAND ----------

# HAVING >= 1000: descarta meses com poucos voos (ex.: voos de 31/dez que partiram já em 1º/jan do ano seguinte)
p4_mes = spark.sql(f"""SELECT ano, mes, nome_mes, {METRICAS} FROM base GROUP BY ano, mes, nome_mes
                       HAVING COUNT(*) >= 1000 ORDER BY ano, mes""")
display(p4_mes)
p4m = p4_mes.toPandas()
p4m["periodo"] = p4m.nome_mes + "/" + p4m.ano.astype(str).str[-2:]
grafico_barras(p4m, "periodo", "pct_atraso_30min", "P4 · % de atraso > 30 min por mês",
               "% dos voos realizados", linha_media=MEDIA_ATRASO)

p4_dia = spark.sql(f"""SELECT dia_semana_num, dia_semana_nome, {METRICAS}
                       FROM base GROUP BY dia_semana_num, dia_semana_nome ORDER BY dia_semana_num""")
display(p4_dia)
grafico_barras(p4_dia.toPandas(), "dia_semana_nome", "pct_atraso_30min",
               "P4 · % de atraso > 30 min por dia da semana", "% dos voos realizados", linha_media=MEDIA_ATRASO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 · Saindo de Brasília (SBBR), quais rotas são mais arriscadas?
# MAGIC Rotas com pelo menos 100 voos programados no período. "Risco" = chance de atraso > 30 min **ou** cancelamento.

# COMMAND ----------

p5 = spark.sql(f"""
SELECT cod_destino, nome_destino, {METRICAS},
       ROUND(100 * AVG(CASE WHEN cancelado OR atrasado_30 THEN 1.0
                            WHEN realizado AND atrasado_30 IS NOT NULL THEN 0.0 END), 2) AS pct_risco_total
FROM base
WHERE cod_origem = '{AEROPORTO_FOCO}'
GROUP BY cod_destino, nome_destino
HAVING COUNT(*) >= 100
ORDER BY pct_risco_total DESC
""")
display(p5)
p5_pd = p5.toPandas().head(15)
p5_pd["rota"] = "BSB → " + p5_pd.nome_destino + " (" + p5_pd.cod_destino + ")"
grafico_barras(p5_pd, "rota", "pct_risco_total", "P5 · Rotas saindo de Brasília: % de atraso > 30 min ou cancelamento",
               "% dos voos com resultado conhecido", horizontal=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Viabilidade financeira: quanto custa operar este pipeline?
# MAGIC Tamanho físico das tabelas na nuvem. Junto com o tempo de execução dos notebooks, isso sustenta a
# MAGIC estimativa de custo do README.

# COMMAND ----------

if not MODO_LOCAL:
    tamanhos = []
    for nome in ["bronze_vra", "silver_voos", "fato_voo", "dim_empresa", "dim_aeroporto", "dim_data"]:
        d = spark.sql(f"DESCRIBE DETAIL {t(nome)}").select("sizeInBytes", "numFiles").first()
        tamanhos.append((nome, round(d.sizeInBytes / 1e6, 2), d.numFiles))
    display(spark.createDataFrame(tamanhos, "tabela STRING, tamanho_mb DOUBLE, arquivos LONG"))
