-- Registering the Gold Delta tables in the Glue Data Catalog for Athena (engine v3),
-- once LAKE_ROOT=s3 points the pipeline at real buckets. Athena reads the schema
-- straight from the _delta_log, so no column list is needed.

CREATE DATABASE IF NOT EXISTS cnpj_gold;

CREATE EXTERNAL TABLE IF NOT EXISTS cnpj_gold.dim_empresas
LOCATION 's3://datacorp-gold/dim_empresas/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS cnpj_gold.fact_estabelecimentos
LOCATION 's3://datacorp-gold/fact_estabelecimentos/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS cnpj_gold.rel_empresa_socio
LOCATION 's3://datacorp-gold/rel_empresa_socio/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS cnpj_gold.agg_empresas_ativas_cnae_uf
LOCATION 's3://datacorp-gold/agg_empresas_ativas_cnae_uf/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- Business question: top 5 CNAEs by active companies in each state.
SELECT uf, rank_cnae_uf, cnae_fiscal_principal, cnae_fiscal_principal_descricao,
       qtd_empresas_ativas, pct_empresas_ativas_uf
FROM cnpj_gold.agg_empresas_ativas_cnae_uf
WHERE rank_cnae_uf <= 5
ORDER BY uf, rank_cnae_uf;
