# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Modelagem → camada Gold (Esquema Estrela)
# MAGIC
# MAGIC **O que este notebook faz:** organiza a Silver num **esquema estrela**, o modelo de Data Warehouse
# MAGIC pedido no trabalho. A tabela **fato** guarda os eventos (cada voo) e as métricas. As **dimensões**
# MAGIC guardam o contexto usado para filtrar e agrupar: quem voou, de onde, para onde e quando.
# MAGIC
# MAGIC ```
# MAGIC                 dim_empresa
# MAGIC                      │
# MAGIC  dim_aeroporto ── fato_voo ── dim_aeroporto
# MAGIC   (origem)           │          (destino)
# MAGIC                   dim_data
# MAGIC ```
# MAGIC
# MAGIC **Decisões de modelagem**
# MAGIC - **Grão da fato:** uma linha = uma etapa de voo programada (ou realizada, se for um voo extra).
# MAGIC - **Chaves das dimensões:** usam códigos naturais estáveis. Empresa e aeroporto usam o **código ICAO**,
# MAGIC   padrão internacional. A data usa `data_id` no formato `AAAAMMDD`. Isso dispensa chaves substitutas e
# MAGIC   deixa as consultas legíveis.
# MAGIC - **`dim_aeroporto` com papel duplo:** a mesma dimensão responde como origem e como destino
# MAGIC   (*role-playing dimension*), sem duplicar dados.
# MAGIC - **Colunas 100% vazias nos arquivos de 2025** (modelo da aeronave, assentos, situação de partida/chegada,
# MAGIC   justificativa) ficam na Silver, para manter a linhagem, mas **não entram na fato**: não há o que analisar nelas.
# MAGIC - **Atributos degenerados** (nº do voo, situação, faixa horária) ficam na própria fato, porque não têm
# MAGIC   descrição adicional que justifique uma dimensão própria.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

silver = spark.table(t("silver_voos"))
FORMATO = "delta" if not MODO_LOCAL else "parquet"


def salvar(df, nome):
    (df.write.format(FORMATO).mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(nome)))
    print(f"✔ {t(nome)}: {spark.table(t(nome)).count():,} linhas")


def nome_mais_frequente(df, col_cod, col_nome):
    """Quando um código aparece com grafias diferentes, fica a mais frequente."""
    contagem = df.filter(F.col(col_cod).isNotNull()).groupBy(col_cod, col_nome).count()
    janela = Window.partitionBy(col_cod).orderBy(F.col("count").desc(), F.col(col_nome).asc_nulls_last())
    return (contagem.withColumn("_r", F.row_number().over(janela)).filter("_r = 1")
                    .select(col_cod, col_nome))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelas de referência (enriquecimento das dimensões)
# MAGIC Os arquivos do VRA de 2025 trazem **só os códigos ICAO**, sem o nome da empresa ou do aeroporto.
# MAGIC Para que os resultados fiquem legíveis ("Brasília" em vez de "SBBR"), as dimensões são enriquecidas
# MAGIC com uma pequena tabela de referência montada a partir dos códigos ICAO públicos. Ela cobre todas as
# MAGIC empresas com mais de 1.000 voos domésticos e todos os aeroportos com mais de 1.000 partidas domésticas.
# MAGIC Códigos fora da lista ficam com o próprio código como nome (`nome_conhecido = false`).

# COMMAND ----------

REF_EMPRESAS = {
    "AZU": ("Azul", "Passageiros"), "TAM": ("LATAM Brasil", "Passageiros"), "GLO": ("Gol", "Passageiros"),
    "ACN": ("Azul Conecta", "Passageiros"), "PTB": ("Voepass (Passaredo)", "Passageiros"),
    "PAM": ("MAP Linhas Aéreas", "Passageiros"), "ABJ": ("Abaeté", "Passageiros"),
    "SID": ("Sideral", "Carga"), "TTL": ("Total Linhas Aéreas", "Carga"), "LTG": ("LATAM Cargo Brasil", "Carga"),
    "MWM": ("Modern Logistics", "Carga"), "TAP": ("TAP Air Portugal", "Passageiros"),
    "LAN": ("LATAM Chile", "Passageiros"), "ARG": ("Aerolíneas Argentinas", "Passageiros"),
    "CMP": ("Copa Airlines", "Passageiros"), "AAL": ("American Airlines", "Passageiros"),
    "UAL": ("United Airlines", "Passageiros"), "AFR": ("Air France", "Passageiros"),
}
REF_AEROPORTOS = {  # código ICAO: (cidade, UF, nome curto)
    "SBAR": ("Aracaju", "SE", "Aracaju"), "SBBE": ("Belém", "PA", "Val de Cans"),
    "SBBR": ("Brasília", "DF", "Brasília (JK)"), "SBBV": ("Boa Vista", "RR", "Boa Vista"),
    "SBCA": ("Cascavel", "PR", "Cascavel"), "SBCF": ("Belo Horizonte", "MG", "Confins"),
    "SBCG": ("Campo Grande", "MS", "Campo Grande"), "SBCH": ("Chapecó", "SC", "Chapecó"),
    "SBCJ": ("Parauapebas", "PA", "Carajás"), "SBCT": ("Curitiba", "PR", "Afonso Pena"),
    "SBCX": ("Caxias do Sul", "RS", "Caxias do Sul"), "SBCY": ("Cuiabá", "MT", "Marechal Rondon"),
    "SBDN": ("Presidente Prudente", "SP", "Presidente Prudente"), "SBEG": ("Manaus", "AM", "Eduardo Gomes"),
    "SBFI": ("Foz do Iguaçu", "PR", "Foz do Iguaçu"), "SBFL": ("Florianópolis", "SC", "Hercílio Luz"),
    "SBFN": ("Fernando de Noronha", "PE", "Fernando de Noronha"), "SBFZ": ("Fortaleza", "CE", "Pinto Martins"),
    "SBGL": ("Rio de Janeiro", "RJ", "Galeão"), "SBGO": ("Goiânia", "GO", "Santa Genoveva"),
    "SBGR": ("São Paulo", "SP", "Guarulhos"), "SBIL": ("Ilhéus", "BA", "Ilhéus"),
    "SBIP": ("Ipatinga", "MG", "Usiminas"), "SBIZ": ("Imperatriz", "MA", "Imperatriz"),
    "SBJP": ("João Pessoa", "PB", "Castro Pinto"), "SBJR": ("Rio de Janeiro", "RJ", "Jacarepaguá"),
    "SBJU": ("Juazeiro do Norte", "CE", "Juazeiro do Norte"), "SBJV": ("Joinville", "SC", "Joinville"),
    "SBKG": ("Campina Grande", "PB", "Campina Grande"), "SBKP": ("Campinas", "SP", "Viracopos"),
    "SBLO": ("Londrina", "PR", "Londrina"), "SBMA": ("Marabá", "PA", "Marabá"),
    "SBMG": ("Maringá", "PR", "Maringá"), "SBMK": ("Montes Claros", "MG", "Montes Claros"),
    "SBMO": ("Maceió", "AL", "Zumbi dos Palmares"), "SBMQ": ("Macapá", "AP", "Macapá"),
    "SBNF": ("Navegantes", "SC", "Navegantes"), "SBPA": ("Porto Alegre", "RS", "Salgado Filho"),
    "SBPJ": ("Palmas", "TO", "Palmas"), "SBPL": ("Petrolina", "PE", "Petrolina"),
    "SBPS": ("Porto Seguro", "BA", "Porto Seguro"), "SBPV": ("Porto Velho", "RO", "Porto Velho"),
    "SBRB": ("Rio Branco", "AC", "Rio Branco"), "SBRF": ("Recife", "PE", "Guararapes"),
    "SBRJ": ("Rio de Janeiro", "RJ", "Santos Dumont"), "SBRP": ("Ribeirão Preto", "SP", "Ribeirão Preto"),
    "SBSG": ("Natal", "RN", "São Gonçalo do Amarante"), "SBSI": ("Sinop", "MT", "Sinop"),
    "SBSL": ("São Luís", "MA", "Marechal Cunha Machado"), "SBSN": ("Santarém", "PA", "Santarém"),
    "SBSP": ("São Paulo", "SP", "Congonhas"), "SBSR": ("São José do Rio Preto", "SP", "Rio Preto"),
    "SBSV": ("Salvador", "BA", "Salvador"), "SBTE": ("Teresina", "PI", "Teresina"),
    "SBUL": ("Uberlândia", "MG", "Uberlândia"), "SBVC": ("Vitória da Conquista", "BA", "Vitória da Conquista"),
    "SBVT": ("Vitória", "ES", "Eurico de Aguiar Salles"),
}
ref_emp = spark.createDataFrame([(k, *v) for k, v in REF_EMPRESAS.items()],
                                "cod_empresa STRING, nome_ref STRING, segmento STRING")
ref_aer = spark.createDataFrame([(k, *v) for k, v in REF_AEROPORTOS.items()],
                                "cod_aeroporto STRING, cidade STRING, uf STRING, nome_ref STRING")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão Empresa

# COMMAND ----------

dim_empresa = (nome_mais_frequente(silver, "cod_empresa", "nome_empresa")
               .join(ref_emp, "cod_empresa", "left")
               .withColumn("nome_conhecido", F.col("nome_ref").isNotNull() | F.col("nome_empresa").isNotNull())
               .withColumn("nome_empresa", F.coalesce("nome_ref", "nome_empresa", "cod_empresa"))
               .withColumn("segmento", F.coalesce("segmento", F.lit("Não classificado")))
               .select("cod_empresa", "nome_empresa", "segmento", "nome_conhecido"))
salvar(dim_empresa, "dim_empresa")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão Aeroporto (origem e destino)

# COMMAND ----------

aeroportos = (silver.select(F.col("cod_origem").alias("cod_aeroporto"), F.col("nome_origem").alias("nome_aeroporto"))
              .unionByName(silver.select(F.col("cod_destino").alias("cod_aeroporto"),
                                         F.col("nome_destino").alias("nome_aeroporto"))))
dim_aeroporto = (nome_mais_frequente(aeroportos, "cod_aeroporto", "nome_aeroporto")
                 .join(ref_aer, "cod_aeroporto", "left")
                 .withColumn("nome_conhecido", F.col("nome_ref").isNotNull() | F.col("nome_aeroporto").isNotNull())
                 .withColumn("nome_aeroporto", F.coalesce("nome_ref", "nome_aeroporto", "cod_aeroporto"))
                 .withColumn("cidade", F.coalesce("cidade", F.lit("Não informada")))
                 .withColumn("uf", F.coalesce("uf", F.lit("--")))
                 .withColumn("aeroporto_brasileiro", F.col("cod_aeroporto").rlike(REGEX_ICAO_BRASIL))
                 .select("cod_aeroporto", "nome_aeroporto", "cidade", "uf", "aeroporto_brasileiro", "nome_conhecido"))
salvar(dim_aeroporto, "dim_aeroporto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão Data

# COMMAND ----------

limites = silver.agg(F.min("data_voo").alias("ini"), F.max("data_voo").alias("fim")).first()
NOMES_MES = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
NOMES_DIA = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"]  # dayofweek: 1 = domingo

dim_data = (spark.sql(f"SELECT explode(sequence(DATE'{limites.ini}', DATE'{limites.fim}', INTERVAL 1 DAY)) AS data")
            .withColumn("data_id", F.date_format("data", "yyyyMMdd").cast("int"))
            .withColumn("ano", F.year("data"))
            .withColumn("trimestre", F.quarter("data"))
            .withColumn("mes", F.month("data"))
            .withColumn("nome_mes", F.element_at(F.array(*[F.lit(m) for m in NOMES_MES]), F.month("data")))
            .withColumn("dia", F.dayofmonth("data"))
            .withColumn("dia_semana_num", F.dayofweek("data"))
            .withColumn("dia_semana_nome", F.element_at(F.array(*[F.lit(d) for d in NOMES_DIA]), F.dayofweek("data")))
            .withColumn("fim_de_semana", F.dayofweek("data").isin(1, 7))
            .select("data_id", "data", "ano", "trimestre", "mes", "nome_mes", "dia",
                    "dia_semana_num", "dia_semana_nome", "fim_de_semana"))
salvar(dim_data, "dim_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela Fato: `fato_voo`

# COMMAND ----------

fato_voo = silver.select(
    "id_voo",
    F.date_format("data_voo", "yyyyMMdd").cast("int").alias("data_id"),
    "cod_empresa", "cod_origem", "cod_destino",
    "numero_voo", "tipo_linha",
    "partida_prevista", "partida_real", "chegada_prevista", "chegada_real",
    "hora_prevista", "faixa_horaria",
    "situacao_voo",
    "realizado", "cancelado", "domestico", "voo_previsto",
    "atraso_partida_min", "atraso_chegada_min", "atraso_valido", "atrasado_30",
    "arquivo_origem",
)
salvar(fato_voo, "fato_voo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste de integridade do modelo
# MAGIC Toda chave estrangeira da fato precisa existir na dimensão correspondente. O resultado esperado é **0** órfãos.

# COMMAND ----------

orfaos = spark.sql(f"""
SELECT 'empresa'  AS dimensao, COUNT(*) AS orfaos FROM {t('fato_voo')} f
  LEFT ANTI JOIN {t('dim_empresa')} d ON f.cod_empresa = d.cod_empresa WHERE f.cod_empresa IS NOT NULL
UNION ALL
SELECT 'origem', COUNT(*) FROM {t('fato_voo')} f
  LEFT ANTI JOIN {t('dim_aeroporto')} d ON f.cod_origem = d.cod_aeroporto WHERE f.cod_origem IS NOT NULL
UNION ALL
SELECT 'destino', COUNT(*) FROM {t('fato_voo')} f
  LEFT ANTI JOIN {t('dim_aeroporto')} d ON f.cod_destino = d.cod_aeroporto WHERE f.cod_destino IS NOT NULL
UNION ALL
SELECT 'data', COUNT(*) FROM {t('fato_voo')} f
  LEFT ANTI JOIN {t('dim_data')} d ON f.data_id = d.data_id WHERE f.data_id IS NOT NULL
""")
display(orfaos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogo de dados dentro do Databricks
# MAGIC Grava a descrição de cada tabela e coluna no **Unity Catalog**. Assim, o catálogo também aparece
# MAGIC na aba *Catalog* da plataforma, não só no GitHub. Vale tirar um print dessa aba como evidência.

# COMMAND ----------

DESCRICOES = {
    "fato_voo": ("Fato: uma linha por etapa de voo do VRA/ANAC, com métricas de atraso", {
        "id_voo": "Hash SHA-256 da chave natural do voo (empresa|voo|origem|destino|horário)",
        "data_id": "Data de referência do voo (AAAAMMDD) - FK dim_data",
        "cod_empresa": "Código ICAO da empresa aérea - FK dim_empresa",
        "cod_origem": "Código ICAO do aeroporto de origem - FK dim_aeroporto",
        "cod_destino": "Código ICAO do aeroporto de destino - FK dim_aeroporto",
        "hora_prevista": "Hora (0-23) da partida prevista",
        "faixa_horaria": "Madrugada/Manhã/Tarde/Noite pela partida prevista (regra R11)",
        "atraso_partida_min": "Partida real - prevista em minutos; nulo se não realizado ou implausível (R4-R6)",
        "atraso_chegada_min": "Chegada real - prevista em minutos; nulo se não realizado ou implausível",
        "atrasado_30": "Verdadeiro se atraso de partida > 30 min (critério ANAC, R7)",
        "domestico": "Doméstico de passageiros: tipo de linha N e origem/destino no Brasil (R8)",
        "voo_previsto": "Possui horário previsto (exclui extras/fretados, R9)",
    }),
    "dim_empresa": ("Dimensão: empresas aéreas", {
        "cod_empresa": "Código ICAO (PK)", "nome_empresa": "Nome comercial (tabela de referência)",
        "segmento": "Passageiros / Carga / Não classificado", "nome_conhecido": "Nome encontrado na referência"}),
    "dim_aeroporto": ("Dimensão: aeroportos (papel de origem e destino)", {
        "cod_aeroporto": "Código ICAO (PK)", "nome_aeroporto": "Nome curto do aeroporto",
        "cidade": "Cidade atendida", "uf": "Unidade da Federação",
        "aeroporto_brasileiro": "Prefixo ICAO brasileiro", "nome_conhecido": "Nome encontrado na referência"}),
    "dim_data": ("Dimensão: calendário", {"data_id": "AAAAMMDD (PK)", "dia_semana_num": "1=Dom ... 7=Sáb"}),
}

if not MODO_LOCAL:
    for tabela, (desc_tabela, colunas) in DESCRICOES.items():
        spark.sql(f"COMMENT ON TABLE {t(tabela)} IS '{desc_tabela}'")
        for coluna, desc in colunas.items():
            spark.sql(f"ALTER TABLE {t(tabela)} ALTER COLUMN {coluna} COMMENT '{desc}'")
    print("Descrições gravadas no Unity Catalog.")
