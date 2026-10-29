# =====================================================================
# GLUE JOB 1  -  BRONZE  ->  SILVER
# Tech Challenge Fase 3 - State of Data Brasil (2023, 2024, 2025)
#
# O que este script faz (a "batedeira" da cozinha):
#   1. Lê os 3 CSVs brutos da camada Bronze no S3
#   2. Descobre o "código da pergunta" de cada coluna (ex.: 1.b = gênero),
#      porque cada ano escreve o nome da coluna de um jeito diferente
#   3. Escolhe só as colunas que respondem às perguntas do projeto
#      e dá a elas um nome simples e igual nos 3 anos
#   4. Limpa e padroniza (tipos, salário numérico, senioridade, etc.)
#   5. Junta os 3 anos numa tabela só e grava em Parquet na Silver,
#      particionada por ano
# =====================================================================

import re
from functools import reduce
from pyspark.sql import SparkSession, functions as F

# ---------------------------------------------------------------------
# 0) CONFIGURAÇÃO  (troque o nome do bucket se for diferente)
# ---------------------------------------------------------------------
BUCKET = "techchallenge-fase3-statedata-geovana"
BRONZE = f"s3://{BUCKET}/bronze"
SILVER = f"s3://{BUCKET}/silver/survey"

ARQUIVOS = {
    2023: f"{BRONZE}/ano=2023/survey_2023.csv",
    2024: f"{BRONZE}/ano=2024/survey_2024.csv",
    2025: f"{BRONZE}/ano=2025/survey_2025.csv",
}

spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()


# ---------------------------------------------------------------------
# 1) DESCOBRIR O CÓDIGO DA PERGUNTA EM CADA COLUNA
#    2023:  "('P2_r ', 'Atualmente qual a sua forma de trabalho?')"  -> "2.r"
#    2024:  "2.r_modelo_de_trabalho_atual"                              -> "2.r"
#    2025:  "4.j.3 Uso e pago pelas soluções..."                        -> "4.j.3"
# ---------------------------------------------------------------------
def codigo_da_coluna(nome):
    if nome.startswith("('P0'"):
        return "0.a"                                   # id do respondente em 2023
    m = re.match(r"\('P(\d)_([a-z])(?:_(\d+))?", nome)  # formato 2023
    if m:
        return ".".join(p for p in m.groups() if p)
    m = re.match(r"(\d\.[a-z](?:\.\d+)?)", nome)        # formato 2024 / 2025
    return m.group(1) if m else None


# ---------------------------------------------------------------------
# 2) O "MAPA": nome novo  ->  código da pergunta em 2023, 2024, 2025
#    (é aqui que resolvemos as diferenças entre os anos)
# ---------------------------------------------------------------------
MAPA = [
    # nome_silver                 2023      2024      2025
    # --- perfil
    ("id_respondente",           "0.a",    "0.a",    "0.a"),
    ("idade",                    "1.a",    "1.a",    "1.a"),
    ("faixa_idade",              "1.a.1",  "1.a.1",  "1.a.1"),
    ("genero",                   "1.b",    "1.b",    "1.b"),
    ("cor_raca_etnia",           "1.c",    "1.c",    "1.c"),
    ("pcd",                      "1.d",    "1.d",    "1.d"),
    ("uf",                       "1.i.1",  "1.i.1",  "1.i.1"),
    ("regiao",                   "1.i.2",  "1.i.2",  "1.i.2"),
    ("nivel_ensino",             "1.l",    "1.l",    "1.l"),
    ("area_formacao",            "1.m",    "1.m",    "1.m"),
    # --- trabalho
    ("situacao_trabalho",        "2.a",    "2.a",    "2.a"),
    ("setor",                    "2.b",    "2.b",    "2.b"),
    ("porte_empresa",            "2.c",    "2.c",    "2.c"),
    ("gestor",                   "2.d",    "2.d",    "2.d"),
    ("cargo",                    "2.f",    "2.f",    "2.f"),
    ("nivel",                    "2.g",    "2.g",    "2.g"),
    ("faixa_salarial",           "2.h",    "2.h",    "2.h"),
    ("tempo_exp_dados",          "2.i",    "2.i",    "2.i"),
    ("satisfeito",               "2.k",    "2.k",    "2.k"),
    ("modelo_trabalho_atual",    "2.r",    "2.r",    "2.q"),   # muda em 2025!
    ("modelo_trabalho_ideal",    "2.s",    "2.s",    "2.r"),   # muda em 2025!
    ("area_atuacao",             "4.a.1",  "4.a.1",  "4.a.1"),
    # --- IA na empresa
    ("ia_prioridade_empresa",    "3.e",    "3.e",    "3.e"),
    # --- linguagens (1 = usa, 0 = não usa)
    ("ling_sql",                 "4.d.1",  "4.d.1",  "4.c.1"),
    ("ling_r",                   "4.d.2",  "4.d.2",  "4.c.2"),
    ("ling_python",              "4.d.3",  "4.d.3",  "4.c.3"),
    ("ling_scala",               "4.d.10", "4.d.10", "4.c.7"),
    # --- cloud (atenção: em 2023 a ordem é diferente!)
    ("cloud_aws",                "4.h.2",  "4.h.1",  "4.e.1"),
    ("cloud_gcp",                "4.h.3",  "4.h.2",  "4.e.2"),
    ("cloud_azure",              "4.h.1",  "4.h.3",  "4.e.3"),
    ("cloud_on_premise",         "4.h.6",  "4.h.6",  "4.e.6"),
    # --- ferramentas de BI
    ("bi_powerbi",               "4.j.1",  "4.j.1",  "4.g.1"),
    ("bi_tableau",               "4.j.3",  "4.j.3",  "4.g.3"),
    ("bi_looker",                "4.j.7",  "4.j.7",  "4.g.7"),
    ("bi_looker_studio",         "4.j.8",  "4.j.8",  "4.g.8"),
    ("bi_quicksight",            "4.j.9",  "4.j.9",  "4.g.9"),
    ("bi_so_planilhas",          "4.j.22", "4.j.17", "4.g.17"),
    # --- IA generativa no dia a dia
    ("ia_nao_usa",               "4.m.1",  "4.m.1",  "4.j.1"),
    ("ia_gratuita",              "4.m.2",  "4.m.2",  "4.j.2"),
    ("ia_paga_propria",          "4.m.3",  "4.m.3",  "4.j.3"),
    ("ia_paga_empresa",          "4.m.4",  "4.m.4",  "4.j.4"),
    ("ia_copilot_codigo",        "4.m.5",  "4.m.5",  "4.j.5"),
]
POSICAO_DO_ANO = {2023: 1, 2024: 2, 2025: 3}

COLUNAS_FLAG = [m[0] for m in MAPA if m[0].startswith(("ling_", "cloud_", "bi_", "ia_"))
                and m[0] != "ia_prioridade_empresa"] + ["gestor", "satisfeito"]


# ---------------------------------------------------------------------
# 3) LER CADA ANO E SELECIONAR AS COLUNAS DO MAPA
# ---------------------------------------------------------------------
def ler_ano(ano, caminho):
    df = (spark.read
          .option("header", True)
          .option("multiLine", True)   # algumas respostas têm quebra de linha
          .option("escape", '"')       # aspas dentro de respostas
          .option("encoding", "UTF-8")
          .csv(caminho))

    # dicionário {código -> nome original da coluna}
    cod_para_coluna = {}
    for nome in df.columns:
        cod = codigo_da_coluna(nome)
        if cod and cod not in cod_para_coluna:
            cod_para_coluna[cod] = nome

    selecionadas = []
    for item in MAPA:
        nome_novo, cod = item[0], item[POSICAO_DO_ANO[ano]]
        original = cod_para_coluna.get(cod)
        if original is None:
            print(f"[AVISO] {ano}: código {cod} ({nome_novo}) não encontrado -> nulo")
            selecionadas.append(F.lit(None).cast("string").alias(nome_novo))
        else:
            # crases (`) porque os nomes têm pontos, parênteses e espaços
            selecionadas.append(F.col(f"`{original}`").cast("string").alias(nome_novo))

    return df.select(*selecionadas).withColumn("ano", F.lit(ano))


df = reduce(lambda a, b: a.unionByName(b),
            [ler_ano(ano, caminho) for ano, caminho in ARQUIVOS.items()])


# ---------------------------------------------------------------------
# 4) LIMPEZA E PADRONIZAÇÃO
# ---------------------------------------------------------------------
# 4.1 tira espaços e transforma texto vazio em nulo
for c in df.columns:
    if c != "ano":
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))))

# 4.2 colunas sim/não viram 0 ou 1 (2024 vem como True/False, os outros como 0/1)
for c in COLUNAS_FLAG:
    v = F.lower(F.col(c))
    df = df.withColumn(c, F.when(v.isin("1", "1.0", "true"), 1)
                           .when(v.isin("0", "0.0", "false"), 0)
                           .otherwise(None).cast("int"))

# 4.3 idade como número
df = df.withColumn("idade", F.col("idade").cast("double").cast("int"))

# 4.4 salário: faixa (texto) -> valor do meio da faixa (número)
#     (inclui 2 faixas com erro de digitação que existem na base original)
SALARIO_MEIO = {
    "Menos de R$ 1.000/mês":             500,
    "de R$ 101/mês a R$ 2.000/mês":      1500,   # erro de digitação na base
    "de R$ 1.001/mês a R$ 2.000/mês":    1500,
    "de R$ 2.001/mês a R$ 3.000/mês":    2500,
    "de R$ 3.001/mês a R$ 4.000/mês":    3500,
    "de R$ 4.001/mês a R$ 6.000/mês":    5000,
    "de R$ 6.001/mês a R$ 8.000/mês":    7000,
    "de R$ 8.001/mês a R$ 12.000/mês":   10000,
    "de R$ 12.001/mês a R$ 16.000/mês":  14000,
    "de R$ 16.001/mês a R$ 20.000/mês":  18000,
    "de R$ 20.001/mês a R$ 25.000/mês":  22500,
    "de R$ 25.001/mês a R$ 30.000/mês":  27500,
    "de R$ 25.001/mês a R$ 3000/mês":    27500,  # erro de digitação na base
    "de R$ 30.001/mês a R$ 40.000/mês":  35000,
    "Acima de R$ 40.001/mês":            45000,
}
mapa_sal = F.create_map(*[x for k, v in SALARIO_MEIO.items() for x in (F.lit(k), F.lit(v))])
df = df.withColumn("salario_estimado", mapa_sal[F.col("faixa_salarial")].cast("int"))

# 4.5 senioridade: gestores não respondem "nível", então viram "Gestor"
df = df.withColumn(
    "senioridade",
    F.when(F.col("nivel").isNotNull(), F.col("nivel"))
     .when(F.col("gestor") == 1, F.lit("Gestor"))
)

# 4.6 modelo de trabalho com nomes curtos
def modelo_curto(col):
    return (F.when(col.contains("100% remoto"), "Remoto")
             .when(col.contains("híbrido flexível"), "Híbrido flexível")
             .when(col.contains("híbrido com dias fixos"), "Híbrido fixo")
             .when(col.contains("100% presencial"), "Presencial")
             .otherwise(col))
df = (df.withColumn("modelo_trabalho_atual", modelo_curto(F.lower(F.col("modelo_trabalho_atual"))))
        .withColumn("modelo_trabalho_ideal", modelo_curto(F.lower(F.col("modelo_trabalho_ideal")))))

# 4.7 indicador geral: a pessoa usa IA generativa no trabalho?
df = df.withColumn(
    "usa_ia_generativa",
    F.when((F.col("ia_gratuita") == 1) | (F.col("ia_paga_propria") == 1) |
           (F.col("ia_paga_empresa") == 1) | (F.col("ia_copilot_codigo") == 1), 1)
     .when(F.col("ia_nao_usa") == 1, 0)
)

# 4.8 remove linhas duplicadas, se houver
df = df.dropDuplicates(["ano", "id_respondente"])


# ---------------------------------------------------------------------
# 5) GRAVAR NA SILVER (Parquet, particionado por ano)
# ---------------------------------------------------------------------
(df.write
   .mode("overwrite")
   .partitionBy("ano")
   .parquet(SILVER))

print("Linhas por ano gravadas na Silver:")
df.groupBy("ano").count().orderBy("ano").show()
