# Evidências da execução na nuvem

Prints que comprovam cada etapa no Databricks. O README principal cita cada um no ponto em que sustenta a afirmação.
Salve as imagens nesta pasta **com exatamente estes nomes**: os links do README já apontam para eles.

| Arquivo | O que mostrar no print |
|---|---|
| `00_fonte_anac.png` | Página de índice da ANAC com as pastas mensais do VRA 2025 (o print que você já tirou) |
| `01_volume_upload.png` | *Catalog → workspace → mvp_voos → raw* com os CSVs enviados (nomes e tamanhos visíveis) |
| `02_log_coleta.png` | Saída da tabela `log_coleta` no notebook 01 |
| `03_funil_transformacao.png` | Saída do funil no notebook 02 |
| `04_integridade_modelo.png` | Tabela de órfãos (todos 0) no notebook 03 |
| `05_unity_catalog.png` | *Catalog → fato_voo → Overview*, mostrando as descrições das colunas |
| `06_qualidade_perfil.png`, `06b_…`, `06c_…` | Tabela de perfil por atributo (notebook 04), em 3 partes |
| `07_qualidade_testes.png` | Tabela de testes Q1–Q12 (notebook 04) |
| `09_atualidade_mensal.png` | Volume por mês (notebook 04, parte 3) |
| `10_validacao_tipo_linha.png` | Validação cruzada tipo de linha × aeroportos (notebook 04, parte 4) |
| `08_tabelas_catalogo.png` | Lista de tabelas do schema `mvp_voos` no Catalog |
| `P1_companhias.png` | Gráfico e tabela da P1 |
| `P2_aeroportos.png` | Gráfico da P2 (Brasília em vermelho) |
| `P3_hora_do_dia.png` | Gráfico de linha da P3 |
| `P3_teste_hipotese.png` | Saída de texto do teste z |
| `P4_sazonalidade.png` | Gráficos da P4 (mês e dia da semana) |
| `P5_rotas_bsb.png` | Gráfico da P5 |
