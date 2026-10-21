# Catálogo de Dados

Este catálogo descreve todas as tabelas da camada **Gold** (esquema estrela) e a linhagem de cada atributo, desde o
arquivo da ANAC até a tabela final. Os **domínios esperados** foram definidos *antes* da análise. O notebook
`04_qualidade_dados` confere se os dados reais os respeitam (tabela `qualidade_testes`).

As mesmas descrições também estão gravadas no **Unity Catalog** do Databricks (aba *Catalog* → `workspace.mvp_voos`).

## Linhagem geral

| Etapa | Onde | O que acontece |
|---|---|---|
| Origem | Portal da ANAC, conjunto **VRA – Voo Regular Ativo** | CSVs mensais (`vra_AAAA_MM.csv`), um registro por etapa de voo |
| Coleta | Volume `/Volumes/workspace/mvp_voos/raw` | Download manual no navegador e upload para o Volume (o site bloqueia robôs) |
| Bronze | `bronze_vra` | Cópia fiel, tudo como texto, colunas renomeadas para nomes canônicos, mais `arquivo_origem` e `data_ingestao` |
| Silver | `silver_voos` | Tipagem, padronização e deduplicação, com as métricas calculadas pelas regras R1–R11 |
| Gold | `fato_voo`, `dim_*` | Esquema estrela para análise |

## Mapeamento de nomes (arquivo original → projeto)

Os arquivos de 2025 usados neste trabalho têm **12 colunas**, marcadas com ★ abaixo. As demais aparecem em arquivos de
outros anos e o leitor também as reconhece. Nos arquivos de 2025 elas ficam vazias na Silver e **não entram na fato**.

| Coluna no CSV da ANAC | Nome no projeto |
|---|---|
| ★ ICAO Empresa Aérea | `cod_empresa` |
| ★ Número Voo | `numero_voo` |
| ★ Código Autorização (DI) | `codigo_di` |
| ★ Código Tipo Linha | `tipo_linha` |
| ★ ICAO Aeródromo Origem / ★ ICAO Aeródromo Destino | `cod_origem` / `cod_destino` |
| ★ Partida Prevista / ★ Partida Real | `partida_prevista` / `partida_real` |
| ★ Chegada Prevista / ★ Chegada Real | `chegada_prevista` / `chegada_real` |
| ★ Situação Voo | `situacao_voo` |
| ★ Código Justificativa | `codigo_justificativa` (100% `N/A` em 2025) |
| *Outros anos:* Sigla ICAO Empresa Aérea / Empresa Aérea | `cod_empresa` / `nome_empresa` |
| *Outros anos:* Sigla ICAO Aeroporto Origem / Descrição Aeroporto Origem | `cod_origem` / `nome_origem` |
| *Outros anos:* Sigla ICAO Aeroporto Destino / Descrição Aeroporto Destino | `cod_destino` / `nome_destino` |
| *Outros anos:* Modelo Equipamento / Número(s) de Assentos | `modelo_equipamento` / `assentos` |
| *Outros anos:* Situação Partida / Situação Chegada | `situacao_partida` / `situacao_chegada` |
| *Outros anos:* Referência / Codeshare | `referencia` / `codeshare` |

---

## `fato_voo`: Tabela fato

**Grão:** uma linha por etapa de voo (um decolar-pousar). **Chave primária:** `id_voo`.

| Atributo | Tipo | Descrição | Domínio esperado | Nulo? (significado) | Linhagem |
|---|---|---|---|---|---|
| `id_voo` | texto | Identificador único do voo | Hash SHA-256 (64 caracteres hexadecimais) | Não | Hash de empresa\|voo\|origem\|destino\|horário de referência (Silver, R10) |
| `data_id` | inteiro | Data de referência do voo, FK de `dim_data` | 20000101 – 20991231 | Sim (data ilegível na origem) | `partida_prevista`, ou `partida_real` se a prevista faltar, em formato AAAAMMDD |
| `cod_empresa` | categórico | Empresa aérea, FK de `dim_empresa` | Código ICAO de 3 letras (ex.: GLO, TAM, AZU) | Não esperado | CSV: *Sigla ICAO Empresa Aérea*, com trim e maiúsculas (R1, R2) |
| `cod_origem` | categórico | Aeroporto de origem, FK de `dim_aeroporto` | Código ICAO de 4 caracteres | Não esperado | CSV: *Sigla ICAO Aeroporto Origem* (R1, R2) |
| `cod_destino` | categórico | Aeroporto de destino, FK de `dim_aeroporto` | Código ICAO de 4 caracteres | Não esperado | CSV: *Sigla ICAO Aeroporto Destino* (R1, R2) |
| `numero_voo` | texto | Número do voo (atributo degenerado) | 1 a 4 dígitos | Não esperado | CSV: *Número Voo* |
| `tipo_linha` | categórico | Tipo de linha da ANAC | N = doméstica mista, C = doméstica cargueira, I = internacional mista, G = internacional cargueira | Raro (463 voos) | CSV: *Código Tipo Linha* |
| `partida_prevista` | data/hora | Partida programada | Dentro do período coletado | Sim, para voo extra/fretado (R9); no CSV vem como o texto `null` | CSV: *Partida Prevista* (R1, R3: formatos aceitos, fração de segundo descartada) |
| `partida_real` | data/hora | Partida efetiva | Dentro do período ± 1 dia | Sim, para voo cancelado | CSV: *Partida Real* (R3) |
| `chegada_prevista` | data/hora | Chegada programada | Após a partida prevista | Sim, para voo extra | CSV: *Chegada Prevista* (R3) |
| `chegada_real` | data/hora | Chegada efetiva | Após a partida real | Sim, para voo cancelado | CSV: *Chegada Real* (R3) |
| `hora_prevista` | numérico | Hora da partida prevista | 0 – 23 | Sim, se não houver partida prevista | `hour(partida_prevista)` |
| `faixa_horaria` | categórico | Período do dia da partida prevista | 1-Madrugada, 2-Manhã, 3-Tarde, 4-Noite | Sim, se não houver partida prevista | Derivado de `hora_prevista` (R11) |
| `situacao_voo` | categórico | Situação final do voo | REALIZADO, CANCELADO | Não esperado | CSV: *Situação Voo* (R2) |
| `realizado` | booleano | Voo foi realizado | verdadeiro/falso | Não | `situacao_voo = 'REALIZADO'` |
| `cancelado` | booleano | Voo foi cancelado | verdadeiro/falso | Não | `situacao_voo = 'CANCELADO'` |
| `domestico` | booleano | Voo doméstico de passageiros | verdadeiro/falso | Não | `tipo_linha = 'N'` e prefixo ICAO SB/SD/SI/SJ/SN/SS/SW nos dois aeroportos (R8) |
| `voo_previsto` | booleano | Tinha horário programado | verdadeiro/falso | Não | `partida_prevista IS NOT NULL` (R9) |
| `atraso_partida_min` | numérico | Atraso de partida em minutos (negativo = adiantado) | −720 a 1440 | Sim, se não realizado, sem horário ou implausível | `partida_real − partida_prevista` (R4, R5, R6) |
| `atraso_chegada_min` | numérico | Atraso de chegada em minutos | −720 a 1440 | Idem | `chegada_real − chegada_prevista` (R4, R5, R6) |
| `atraso_valido` | booleano | Atraso calculado estava dentro da faixa plausível | verdadeiro/falso | Sim, se não houve atraso a calcular | Regra R6 |
| `atrasado_30` | booleano | Partida com mais de 30 min de atraso | verdadeiro/falso | Sim, se o atraso for nulo | `atraso_partida_min > 30` (R7) |
| `arquivo_origem` | texto | Arquivo CSV de onde veio a linha | `vra_AAAA_MM.csv` | Não | Adicionado na Bronze (linhagem) |

## `dim_empresa`: Dimensão empresa aérea

| Atributo | Tipo | Descrição | Domínio esperado | Nulo? | Linhagem |
|---|---|---|---|---|---|
| `cod_empresa` | categórico | Código ICAO da empresa (**PK**) | 3 caracteres | Não | Valores distintos de `silver_voos.cod_empresa` |
| `nome_empresa` | texto | Nome comercial | Texto livre | Não (usa o próprio código se não houver referência) | Tabela de referência `REF_EMPRESAS` (notebook 03), porque os CSVs de 2025 não trazem nomes |
| `segmento` | categórico | Tipo de operação | Passageiros, Carga, Não classificado | Não | Tabela de referência |
| `nome_conhecido` | booleano | O nome veio da referência | verdadeiro/falso | Não | `nome_ref IS NOT NULL` |

## `dim_aeroporto`: Dimensão aeroporto (papel de origem e destino)

| Atributo | Tipo | Descrição | Domínio esperado | Nulo? | Linhagem |
|---|---|---|---|---|---|
| `cod_aeroporto` | categórico | Código ICAO do aeroporto (**PK**) | 4 caracteres | Não | União de `cod_origem` e `cod_destino` |
| `nome_aeroporto` | texto | Nome curto do aeroporto | Texto livre | Não (usa o próprio código se não houver referência) | Tabela de referência `REF_AEROPORTOS` (notebook 03) |
| `cidade` | texto | Cidade atendida | Texto livre | Não ("Não informada") | Tabela de referência |
| `uf` | categórico | Unidade da Federação | 27 siglas, ou "--" para estrangeiro/desconhecido | Não | Tabela de referência |
| `nome_conhecido` | booleano | O nome veio da referência | verdadeiro/falso | Não | `nome_ref IS NOT NULL` |
| `aeroporto_brasileiro` | booleano | Prefixo ICAO brasileiro | verdadeiro/falso | Não | Regex `^S[BDIJNSW]..$` |

## `dim_data`: Dimensão calendário

| Atributo | Tipo | Descrição | Domínio esperado | Nulo? | Linhagem |
|---|---|---|---|---|---|
| `data_id` | inteiro | Data no formato AAAAMMDD (**PK**) | Período coletado, sem lacunas | Não | Gerada com `sequence()` entre a menor e a maior data da Silver |
| `data` | data | Data | Idem | Não | Idem |
| `ano` | numérico | Ano | Anos do período | Não | `year(data)` |
| `trimestre` | numérico | Trimestre | 1 – 4 | Não | `quarter(data)` |
| `mes` | numérico | Mês | 1 – 12 | Não | `month(data)` |
| `nome_mes` | categórico | Mês abreviado | Jan … Dez | Não | Tabela fixa |
| `dia` | numérico | Dia do mês | 1 – 31 | Não | `dayofmonth(data)` |
| `dia_semana_num` | numérico | Dia da semana | 1 (Dom) – 7 (Sáb) | Não | `dayofweek(data)` |
| `dia_semana_nome` | categórico | Dia da semana abreviado | Dom, Seg, Ter, Qua, Qui, Sex, Sáb | Não | Tabela fixa |
| `fim_de_semana` | booleano | Sábado ou domingo | verdadeiro/falso | Não | `dia_semana_num IN (1, 7)` |

## Tabelas de apoio (evidência e governança)

| Tabela | Conteúdo |
|---|---|
| `log_coleta` | Arquivo, linhas, tamanho, encoding, separador e data de ingestão de cada CSV |
| `log_funil_transformacao` | Quantas linhas restam após cada regra da Silver |
| `qualidade_perfil` | Perfil de cada atributo (nulos, distintos, mín./máx., valores mais frequentes) |
| `qualidade_testes` | Resultado dos testes de qualidade Q1–Q12 e o tratamento de cada um |
