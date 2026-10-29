# =====================================================================
# GLUE JOB 2  -  SILVER  ->  GOLD
# Tech Challenge Fase 3 - State of Data Brasil (2023, 2024, 2025)
#
# A Silver tem 1 linha por pessoa. A Gold tem tabelas pequenas e já
# resumidas, uma para cada pergunta de negócio. Cada tabela Gold vira
# um (ou mais) gráfico(s) na apresentação.
#
#   gold_kpis               -> números gerais por ano (capa / resumo)
#   gold_cargos             -> como o mercado está estruturado
#   gold_senioridade        -> perfis mais valorizados (salário)
#   gold_genero             -> diversidade de gênero e diferença salarial
#   gold_tecnologias        -> tecnologias mais adotadas
#   gold_ia_uso             -> adoção de IA generativa pelos profissionais
#   gold_ia_empresas        -> IA como prioridade nas empresas
#   gold_regiao             -> diferenças entre regiões
#   gold_modelo_trabalho    -> remoto x híbrido x presencial
# =====================================================================

from pyspark.sql import SparkSession, functions as F, Window

BUCKET = "techchallenge-fase3-statedata-geovana"
SILVER = f"s3://{BUCKET}/silver/survey"
GOLD = f"s3://{BUCKET}/gold"

spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()
df = spark.read.parquet(SILVER)

# Funções de apoio -----------------------------------------------------
def pct(col):
    """Média de uma coluna 0/1 = porcentagem de quem marcou."""
    return F.round(F.avg(col) * 100, 1)

def sal_medio():
    return F.round(F.avg("salario_estimado"), 0)

def sal_mediano():
    return F.percentile_approx("salario_estimado", 0.5)

def pct_do_total(particao="ano"):
    """% que cada linha representa dentro do ano."""
    w = Window.partitionBy(particao)
    return F.round(F.col("qtd") / F.sum("qtd").over(w) * 100, 1)

def salvar(tabela, nome):
    tabela.write.mode("overwrite").parquet(f"{GOLD}/{nome}")
    print(f"[OK] {nome}: {tabela.count()} linhas")

empregados = df.filter(F.col("salario_estimado").isNotNull())


# 1) KPIs gerais por ano ----------------------------------------------
kpis = df.groupBy("ano").agg(
    F.count("*").alias("respondentes"),
    F.round(F.avg("idade"), 1).alias("idade_media"),
    pct((F.col("genero") == "Feminino").cast("int")).alias("pct_mulheres"),
    sal_medio().alias("salario_medio"),
    sal_mediano().alias("salario_mediano"),
    pct("usa_ia_generativa").alias("pct_usa_ia"),
    pct((F.col("modelo_trabalho_atual") == "Remoto").cast("int")).alias("pct_remoto"),
    pct("ling_python").alias("pct_python"),
    pct("cloud_aws").alias("pct_aws"),
)
salvar(kpis, "gold_kpis")


# 2) Estrutura do mercado: cargos -------------------------------------
cargos = (empregados.filter(F.col("cargo").isNotNull())
          .groupBy("ano", "cargo")
          .agg(F.count("*").alias("qtd"),
               sal_medio().alias("salario_medio"),
               sal_mediano().alias("salario_mediano"))
          .withColumn("pct_do_ano", pct_do_total()))
salvar(cargos, "gold_cargos")


# 3) Perfis mais valorizados: senioridade -----------------------------
senioridade = (empregados.filter(F.col("senioridade").isNotNull())
               .groupBy("ano", "senioridade")
               .agg(F.count("*").alias("qtd"),
                    sal_medio().alias("salario_medio"),
                    sal_mediano().alias("salario_mediano"),
                    pct("usa_ia_generativa").alias("pct_usa_ia"))
               .withColumn("pct_do_ano", pct_do_total())
               .withColumn("ordem", F.when(F.col("senioridade") == "Júnior", 1)
                                     .when(F.col("senioridade") == "Pleno", 2)
                                     .when(F.col("senioridade") == "Sênior", 3)
                                     .when(F.col("senioridade") == "Especialista/Staff+", 4)
                                     .when(F.col("senioridade") == "Gestor", 5)))
salvar(senioridade, "gold_senioridade")


# 4) Diversidade de gênero --------------------------------------------
genero = (df.filter(F.col("genero").isin("Masculino", "Feminino"))
          .groupBy("ano", "genero", "senioridade")
          .agg(F.count("*").alias("qtd"),
               sal_medio().alias("salario_medio")))
salvar(genero, "gold_genero")


# 5) Tecnologias: formato "longo" (uma linha por tecnologia) ----------
TECNOLOGIAS = [
    ("Linguagem", "SQL", "ling_sql"), ("Linguagem", "Python", "ling_python"),
    ("Linguagem", "R", "ling_r"), ("Linguagem", "Scala", "ling_scala"),
    ("Cloud", "AWS", "cloud_aws"), ("Cloud", "Azure", "cloud_azure"),
    ("Cloud", "GCP", "cloud_gcp"), ("Cloud", "On premise", "cloud_on_premise"),
    ("BI", "Power BI", "bi_powerbi"), ("BI", "Tableau", "bi_tableau"),
    ("BI", "Looker", "bi_looker"), ("BI", "Looker Studio", "bi_looker_studio"),
    ("BI", "QuickSight", "bi_quicksight"), ("BI", "Só planilhas", "bi_so_planilhas"),
]
tecnologias = None
for categoria, nome, coluna in TECNOLOGIAS:
    parte = (df.groupBy("ano")
               .agg(pct(coluna).alias("pct_uso"),
                    F.count(coluna).alias("respondentes"))
               .withColumn("categoria", F.lit(categoria))
               .withColumn("tecnologia", F.lit(nome)))
    tecnologias = parte if tecnologias is None else tecnologias.unionByName(parte)
salvar(tecnologias.select("ano", "categoria", "tecnologia", "pct_uso", "respondentes"),
       "gold_tecnologias")


# 6) IA generativa: uso pelos profissionais ---------------------------
ia_uso = (df.filter(F.col("usa_ia_generativa").isNotNull())
          .groupBy("ano")
          .agg(F.count("*").alias("respondentes"),
               pct("usa_ia_generativa").alias("pct_usa_ia"),
               pct("ia_nao_usa").alias("pct_nao_usa"),
               pct("ia_gratuita").alias("pct_so_gratuita"),
               pct("ia_paga_propria").alias("pct_paga_do_bolso"),
               pct("ia_paga_empresa").alias("pct_empresa_paga"),
               pct("ia_copilot_codigo").alias("pct_ia_para_codigo")))
salvar(ia_uso, "gold_ia_uso")


# 7) IA generativa: prioridade nas empresas (pergunta feita a gestores)
ia_empresas = (df.filter(F.col("ia_prioridade_empresa").isNotNull())
               .withColumn("prioridade", F.when(F.col("ia_prioridade_empresa").startswith("Sim, é nossa principal"), "Principal prioridade")
                                          .when(F.col("ia_prioridade_empresa").startswith("Sim, está entre"), "Entre as prioridades")
                                          .when(F.col("ia_prioridade_empresa").startswith("Mais ou menos"), "Iniciativas isoladas")
                                          .when(F.col("ia_prioridade_empresa").startswith("Não é"), "Não é prioridade")
                                          .otherwise("Não sabe"))
               .groupBy("ano", "prioridade").agg(F.count("*").alias("qtd"))
               .withColumn("pct_do_ano", pct_do_total()))
salvar(ia_empresas, "gold_ia_empresas")


# 8) Regiões ----------------------------------------------------------
regiao = (df.filter(F.col("regiao").isNotNull())
          .groupBy("ano", "regiao")
          .agg(F.count("*").alias("qtd"),
               sal_medio().alias("salario_medio"),
               pct((F.col("modelo_trabalho_atual") == "Remoto").cast("int")).alias("pct_remoto"),
               pct((F.col("genero") == "Feminino").cast("int")).alias("pct_mulheres"))
          .withColumn("pct_do_ano", pct_do_total()))
salvar(regiao, "gold_regiao")


# 9) Modelo de trabalho: atual x ideal --------------------------------
atual = (df.filter(F.col("modelo_trabalho_atual").isNotNull())
         .groupBy("ano", F.col("modelo_trabalho_atual").alias("modelo"))
         .agg(F.count("*").alias("qtd"), sal_medio().alias("salario_medio"))
         .withColumn("pct_do_ano", pct_do_total())
         .withColumn("tipo", F.lit("Atual")))
ideal = (df.filter(F.col("modelo_trabalho_ideal").isNotNull())
         .groupBy("ano", F.col("modelo_trabalho_ideal").alias("modelo"))
         .agg(F.count("*").alias("qtd"), F.lit(None).cast("double").alias("salario_medio"))
         .withColumn("pct_do_ano", pct_do_total())
         .withColumn("tipo", F.lit("Ideal")))
salvar(atual.unionByName(ideal), "gold_modelo_trabalho")

print("Camada Gold concluída!")
