-- =====================================================================
-- TECH CHALLENGE FASE 3 · CONSULTAS ANALÍTICAS NO AMAZON ATHENA
-- Database: statedata_db  (tabelas criadas pelos crawlers)
--   silver_survey  -> 1 linha por respondente, 3 anos, particionada por ano
--   gold_*         -> tabelas agregadas, uma por pergunta de negócio
-- Como usar: no Athena, escolha o database statedata_db e rode
-- uma consulta por vez (selecione o bloco e clique em Executar).
-- =====================================================================


-- ---------------------------------------------------------------------
-- 0. VALIDAÇÃO DO PIPELINE: quantas respostas por ano chegaram na Silver
-- ---------------------------------------------------------------------
SELECT ano, COUNT(*) AS respondentes
FROM silver_survey
GROUP BY ano
ORDER BY ano;


-- ---------------------------------------------------------------------
-- 0.1 VISÃO GERAL (KPIs por ano)
-- ---------------------------------------------------------------------
SELECT *
FROM gold_kpis
ORDER BY ano;


-- =====================================================================
-- P1. COMO ESTÁ ESTRUTURADO O MERCADO BRASILEIRO DE DADOS?
-- =====================================================================

-- 1.1 Cargos mais comuns em 2025 (participação e salário)
SELECT cargo, qtd, pct_do_ano, salario_medio
FROM gold_cargos
WHERE ano = 2025
ORDER BY qtd DESC
LIMIT 10;

-- 1.2 Distribuição por senioridade ao longo dos anos
SELECT ano, senioridade, qtd, pct_do_ano
FROM gold_senioridade
ORDER BY ano, ordem;

-- 1.3 Setores que mais empregam profissionais de dados (2025)
SELECT setor,
       COUNT(*) AS qtd,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM silver_survey
WHERE ano = 2025 AND setor IS NOT NULL
GROUP BY setor
ORDER BY qtd DESC
LIMIT 10;

-- 1.4 Escolaridade dos profissionais (2025)
SELECT nivel_ensino,
       COUNT(*) AS qtd,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM silver_survey
WHERE ano = 2025 AND nivel_ensino IS NOT NULL
GROUP BY nivel_ensino
ORDER BY qtd DESC;


-- =====================================================================
-- P2. QUAIS PERFIS PROFISSIONAIS SÃO MAIS VALORIZADOS?
-- =====================================================================

-- 2.1 Salário médio por senioridade e ano
SELECT ano, senioridade, salario_medio, salario_mediano
FROM gold_senioridade
ORDER BY ano, ordem;

-- 2.2 Cargos com maior salário médio em 2025 (mínimo de 30 respondentes)
SELECT cargo, qtd, salario_medio
FROM gold_cargos
WHERE ano = 2025 AND qtd >= 30
ORDER BY salario_medio DESC
LIMIT 10;

-- 2.3 Evolução do salário médio geral (crescimento % ano a ano)
SELECT ano,
       salario_medio,
       ROUND((salario_medio - LAG(salario_medio) OVER (ORDER BY ano))
             * 100.0 / LAG(salario_medio) OVER (ORDER BY ano), 1) AS cresc_pct
FROM gold_kpis
ORDER BY ano;


-- =====================================================================
-- P3. QUAL É O CENÁRIO DE DIVERSIDADE DE GÊNERO?
-- =====================================================================

-- 3.1 Participação feminina por ano
SELECT ano, pct_mulheres
FROM gold_kpis
ORDER BY ano;

-- 3.2 Diferença salarial entre homens e mulheres por senioridade
SELECT ano,
       senioridade,
       MAX(CASE WHEN genero = 'Masculino' THEN salario_medio END) AS sal_homens,
       MAX(CASE WHEN genero = 'Feminino'  THEN salario_medio END) AS sal_mulheres,
       ROUND((MAX(CASE WHEN genero = 'Masculino' THEN salario_medio END)
            - MAX(CASE WHEN genero = 'Feminino'  THEN salario_medio END)) * 100.0
            / MAX(CASE WHEN genero = 'Masculino' THEN salario_medio END), 1) AS gap_pct
FROM gold_genero
WHERE senioridade IS NOT NULL
GROUP BY ano, senioridade
ORDER BY ano, senioridade;

-- 3.3 Participação feminina por nível (2025): elas chegam aos cargos altos?
SELECT senioridade,
       SUM(qtd) AS total,
       ROUND(SUM(CASE WHEN genero = 'Feminino' THEN qtd ELSE 0 END) * 100.0 / SUM(qtd), 1) AS pct_mulheres
FROM gold_genero
WHERE ano = 2025 AND senioridade IS NOT NULL
GROUP BY senioridade
ORDER BY pct_mulheres DESC;


-- =====================================================================
-- P4. QUAIS TECNOLOGIAS TÊM MAIOR ADOÇÃO?
-- =====================================================================

-- 4.1 Ranking de tecnologias em 2025 por categoria
SELECT categoria, tecnologia, pct_uso
FROM gold_tecnologias
WHERE ano = 2025
ORDER BY categoria, pct_uso DESC;

-- 4.2 Evolução das clouds (2023 -> 2025)
SELECT tecnologia,
       MAX(CASE WHEN ano = 2023 THEN pct_uso END) AS pct_2023,
       MAX(CASE WHEN ano = 2024 THEN pct_uso END) AS pct_2024,
       MAX(CASE WHEN ano = 2025 THEN pct_uso END) AS pct_2025
FROM gold_tecnologias
WHERE categoria = 'Cloud'
GROUP BY tecnologia
ORDER BY pct_2025 DESC;


-- =====================================================================
-- P5. ADOÇÃO DE INTELIGÊNCIA ARTIFICIAL E SEU IMPACTO
-- =====================================================================

-- 5.1 Uso de IA generativa pelos profissionais
SELECT *
FROM gold_ia_uso
ORDER BY ano;

-- 5.2 IA como prioridade nas empresas (resposta de gestores)
SELECT ano, prioridade, qtd, pct_do_ano
FROM gold_ia_empresas
ORDER BY ano, pct_do_ano DESC;

-- 5.3 Impacto: quem tem a IA paga pela empresa ganha mais? (2025)
SELECT CASE WHEN ia_paga_empresa = 1 THEN 'Empresa paga a IA'
            ELSE 'Empresa não paga' END AS grupo,
       COUNT(*) AS qtd,
       ROUND(AVG(salario_estimado), 0) AS salario_medio
FROM silver_survey
WHERE ano = 2025 AND usa_ia_generativa IS NOT NULL AND salario_estimado IS NOT NULL
GROUP BY 1;


-- =====================================================================
-- P6. DIFERENÇAS ENTRE REGIÕES, SENIORIDADES E MODELOS DE TRABALHO
-- =====================================================================

-- 6.1 Regiões: concentração, salário e remoto
SELECT ano, regiao, qtd, pct_do_ano, salario_medio, pct_remoto
FROM gold_regiao
ORDER BY ano, qtd DESC;

-- 6.2 Modelo de trabalho atual x ideal (2025)
SELECT modelo,
       MAX(CASE WHEN tipo = 'Atual' THEN pct_do_ano END) AS pct_atual,
       MAX(CASE WHEN tipo = 'Ideal' THEN pct_do_ano END) AS pct_ideal,
       MAX(CASE WHEN tipo = 'Atual' THEN salario_medio END) AS salario_medio
FROM gold_modelo_trabalho
WHERE ano = 2025
GROUP BY modelo
ORDER BY pct_atual DESC;

-- 6.3 Evolução do trabalho 100% remoto
SELECT ano, pct_remoto
FROM gold_kpis
ORDER BY ano;


-- =====================================================================
-- P7. OPORTUNIDADES E DESAFIOS (consultas de apoio às recomendações)
-- =====================================================================

-- 7.1 Satisfação por modelo de trabalho (risco de retenção) · 2025
SELECT modelo_trabalho_atual,
       COUNT(*) AS qtd,
       ROUND(AVG(satisfeito) * 100, 1) AS pct_satisfeitos
FROM silver_survey
WHERE ano = 2025 AND modelo_trabalho_atual IS NOT NULL AND satisfeito IS NOT NULL
GROUP BY modelo_trabalho_atual
ORDER BY pct_satisfeitos DESC;

-- 7.2 Perfis escassos e caros: cargos com pouca oferta e salário alto (2025)
SELECT cargo, qtd, pct_do_ano, salario_medio
FROM gold_cargos
WHERE ano = 2025 AND qtd >= 20 AND pct_do_ano < 6
ORDER BY salario_medio DESC
LIMIT 5;
