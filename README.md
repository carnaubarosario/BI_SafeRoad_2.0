🚦 Análise de Acidentes Rodoviários no Brasil
Data Warehouse + SQL + Power BI

📌 Sobre o Projeto

Este projeto consiste na construção de um Data Warehouse em PostgreSQL para análise de acidentes rodoviários no Brasil, seguido do desenvolvimento de métricas analíticas e dashboard no Power BI.

O objetivo é transformar dados brutos em informação estratégica, permitindo:

Análise de vítimas

Taxas de mortalidade

Distribuição por sexo, idade e tipo de veículo

Comparações por estado e ano

Identificação de padrões de risco

🏗️ Arquitetura do Projeto
🔹 Modelo Dimensional (Star Schema)

O DW foi estruturado seguindo modelagem dimensional.

📊 Tabela Fato

fato_acidentes

📚 Dimensões

dim_tempo

dim_vitima

dim_pista

dim_acidente

dim_veiculo

dim_localidade

🗄️ Estrutura do Data Warehouse
🎯 Tabela Fato
fato_acidentes (
  id_fato,
  id_tempo,
  id_vitima,
  id_pista,
  id_acidente,
  id_veiculo,
  id_localidade,
  ilesos,
  feridos_leves,
  feridos_graves,
  mortos
)

📐 Dimensões

dim_tempo

data_completa

ano

mes

trimestre

nome_mes

dia_semana

fase_dia

dim_vitima

sexo

idade

estado_fisico

tipo_envolvido

dim_acidente

tipo_acidente

classificacao_acidente

dim_veiculo

tipo_veiculo

marca

ano_fabricacao

dim_localidade

municipio

uf

br

km

latitude

longitude

📈 Principais Métricas Desenvolvidas
🔹 Total de Vítimas

Soma de:

Ilesos

Feridos leves

Feridos graves

Mortos

SELECT
  COALESCE(SUM(ilesos),0)
+ COALESCE(SUM(feridos_leves),0)
+ COALESCE(SUM(feridos_graves),0)
+ COALESCE(SUM(mortos),0) AS total_vitimas
FROM fato_acidentes;

🔹 Vítimas Fatais
SELECT SUM(mortos) AS vitimas_fatais
FROM fato_acidentes;

🔹 Taxa de Mortalidade
SELECT
  SUM(mortos)::numeric /
  NULLIF(
    SUM(ilesos) +
    SUM(feridos_leves) +
    SUM(feridos_graves) +
    SUM(mortos),
    0
  ) AS taxa_mortalidade
FROM fato_acidentes;

🔹 Média de Acidentes por Estado
SELECT AVG(acidentes_por_uf)
FROM (
  SELECT l.uf,
         COUNT(*) AS acidentes_por_uf
  FROM fato_acidentes f
  JOIN dim_localidade l ON l.id_localidade = f.id_localidade
  GROUP BY l.uf
)

📊 Dashboard Power BI

O dashboard foi desenvolvido com foco em:

KPIs executivos

Análise temporal

Distribuição geográfica

Segmentação por perfil de vítima

Análise por tipo de veículo

Indicadores principais:

Total de vítimas

Vítimas fatais

Taxa de mortalidade

Acidentes por UF

Acidentes por tipo de veículo

Distribuição por sexo

Faixa etária mais envolvida

🛠️ Tecnologias Utilizadas

PostgreSQL

SQL (Analytics Queries)

Modelagem Dimensional

Power BI

Git & GitHub

🎯 Objetivos Técnicos do Projeto

✔ Construção de DW relacional
✔ Tradução de medidas DAX para SQL
✔ Garantia de consistência entre banco e dashboard
✔ Aplicação de boas práticas de modelagem
✔ Criação de métricas analíticas robustas

🚀 Possíveis Evoluções

Implementação de ETL automatizado

Camadas Bronze / Silver / Gold

Materialized Views para performance

Integração com API pública de dados

Deploy em ambiente cloud (GCP / AWS)

👨‍💻 Autor

Lucca Carnaúba Peixoto Rosário
Analista de BI | Modelagem Dimensional | Engenharia de Dados
