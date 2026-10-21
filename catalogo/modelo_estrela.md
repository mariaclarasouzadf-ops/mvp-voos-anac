# Modelo de dados: Esquema Estrela

O GitHub desenha o diagrama abaixo automaticamente (sintaxe Mermaid).

```mermaid
erDiagram
    dim_empresa   ||--o{ fato_voo : "opera"
    dim_aeroporto ||--o{ fato_voo : "origem"
    dim_aeroporto ||--o{ fato_voo : "destino"
    dim_data      ||--o{ fato_voo : "acontece em"

    fato_voo {
        string    id_voo PK
        int       data_id FK
        string    cod_empresa FK
        string    cod_origem FK
        string    cod_destino FK
        string    numero_voo
        timestamp partida_prevista
        timestamp partida_real
        int       hora_prevista
        string    faixa_horaria
        string    situacao_voo
        boolean   realizado
        boolean   cancelado
        boolean   domestico
        double    atraso_partida_min
        double    atraso_chegada_min
        boolean   atrasado_30
    }
    dim_empresa {
        string cod_empresa PK
        string nome_empresa
    }
    dim_aeroporto {
        string  cod_aeroporto PK
        string  nome_aeroporto
        boolean aeroporto_brasileiro
    }
    dim_data {
        int     data_id PK
        date    data
        int     ano
        int     mes
        string  nome_mes
        int     dia_semana_num
        string  dia_semana_nome
        boolean fim_de_semana
    }
```

## Por que um esquema estrela?

- **As perguntas de negócio são todas do tipo "taxa de atraso por X"**: por empresa, por aeroporto, por hora, por mês.
  O esquema estrela foi feito para isso. A métrica fica na fato e cada "X" é uma dimensão.
- **Consultas simples:** toda resposta é um `JOIN` da fato com uma ou duas dimensões e um `GROUP BY`.
- **Não precisa de Snowflake:** as dimensões são pequenas (dezenas de empresas, centenas de aeroportos).
  Normalizá-las em hierarquias não traria ganho e deixaria as consultas mais longas.
- **`dim_aeroporto` com papel duplo** (*role-playing dimension*): a mesma tabela é usada duas vezes, uma como origem
  e outra como destino, sem duplicar dados.

Pipeline completo (camadas):

```mermaid
flowchart LR
    A["Portal ANAC<br/>CSVs mensais VRA"] -->|download manual| B["Volume Databricks<br/>/Volumes/.../raw"]
    B -->|01_coleta_bronze| C[("bronze_vra<br/>cópia fiel, texto")]
    C -->|"02_transformacao_silver<br/>regras R1–R11"| D[("silver_voos<br/>tipada e deduplicada")]
    D -->|03_modelagem_gold| E[("fato_voo + dim_*<br/>esquema estrela")]
    D -->|04_qualidade_dados| F[("qualidade_perfil<br/>qualidade_testes")]
    E -->|05_analise| G["Respostas P1–P5<br/>tabelas e gráficos"]
```
