"""
Geração dos gráficos da apresentação a partir das tabelas GOLD.

Uso:
    from graficos import gerar_todos
    gerar_todos(tabelas, pasta_saida="graficos")
onde `tabelas` é um dicionário {"gold_kpis": DataFrame, ...} (pandas).
"""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

# ---- Paleta (validada para daltonismo nas 3 primeiras cores) ----------
AZUL, LARANJA, VERDE_AGUA = "#2a78d6", "#eb6834", "#1baf7a"
CINZA = "#c9c8c2"          # categorias de contexto (não destacadas)
TXT, TXT2, GRADE = "#0b0b0b", "#52514e", "#e6e5e0"
COR_ANO = {2023: CINZA, 2024: "#86b6ef", 2025: AZUL}   # anos = rampa de um tom só

plt.rcParams.update({
    "font.family": "Inter", "font.size": 15, "text.color": TXT,
    "axes.edgecolor": GRADE, "axes.labelcolor": TXT2, "xtick.color": TXT2, "ytick.color": TXT2,
    "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False,
    "axes.grid": False, "figure.facecolor": "white", "axes.facecolor": "white",
    "savefig.dpi": 200, "legend.frameon": False,
})

def _brl(v):
    return f"R$ {v/1000:.1f} mil".replace(".", ",")

def _pct(v):
    return f"{v:.0f}%" if abs(v) >= 10 else f"{v:.1f}%".replace(".", ",")

def _salvar(fig, pasta, nome):
    fig.tight_layout()
    caminho = os.path.join(pasta, nome)
    fig.savefig(caminho, bbox_inches="tight", pad_inches=0.2)
    plt.close(fig)
    return caminho

def _barras_h(ax, rotulos, valores, cores, fmt, maxv=None):
    """Barras horizontais com valor escrito na ponta (ranking)."""
    y = range(len(rotulos))[::-1]
    ax.barh(list(y), valores, color=cores, height=0.62)
    maxv = maxv or max(valores)
    for yi, v in zip(y, valores):
        ax.text(v + maxv * 0.012, yi, fmt(v), va="center", fontsize=14, color=TXT)
    ax.set_yticks(list(y)); ax.set_yticklabels(rotulos, fontsize=14, color=TXT)
    ax.set_xticks([]); ax.spines["bottom"].set_visible(False)
    ax.set_xlim(0, maxv * 1.15)
    ax.tick_params(axis="y", length=0)

NOMES_CARGO = {
    "Analista de Dados/Data Analyst": "Analista de Dados",
    "Cientista de Dados/Data Scientist": "Cientista de Dados",
    "Engenheiro de Dados/Data Engineer/Data Architect": "Engenheiro de Dados",
    "Analista de BI/BI Analyst": "Analista de BI",
    "Analista de Negócios/Business Analyst": "Analista de Negócios",
    "Analytics Engineer": "Analytics Engineer",
    "Engenheiro de Machine Learning/ML Engineer/AI Engineer": "Eng. de ML / IA",
    "Desenvolvedor/ Engenheiro de Software/ Analista de Sistemas": "Dev / Eng. de Software",
    "Data Product Manager/ Product Manager (PM/APM/DPM/GPM/PO)": "Product Manager de Dados",
    "Arquiteto de Dados/Data Architect": "Arquiteto de Dados",
    "Analista de Suporte/Analista Técnico": "Analista de Suporte",
}


def g01_cargos(t, pasta):
    d = t["gold_cargos"].query("ano == 2025 and cargo != 'Outra Opção'").nlargest(8, "qtd")
    fig, ax = plt.subplots(figsize=(10, 5.6))
    cores = [AZUL if i < 3 else CINZA for i in range(len(d))]
    _barras_h(ax, [NOMES_CARGO.get(c, c) for c in d.cargo], d.pct_do_ano.tolist(), cores, _pct)
    return _salvar(fig, pasta, "g01_cargos_2025.png")


def g02_setores(silver_setor, pasta):
    d = silver_setor.query("setor != 'Outra Opção'").head(8)
    fig, ax = plt.subplots(figsize=(10, 5.6))
    cores = [LARANJA if "Finan" in s else CINZA for s in d.setor]
    _barras_h(ax, d.setor.tolist(), d.pct.tolist(), cores, _pct)
    return _salvar(fig, pasta, "g02_setores_2025.png")


def g03_salario_senioridade(t, pasta):
    d = t["gold_senioridade"].sort_values(["ordem", "ano"])
    niveis = ["Júnior", "Pleno", "Sênior", "Especialista/Staff+", "Gestor"]
    fig, ax = plt.subplots(figsize=(11, 5.6))
    w = 0.26
    for i, ano in enumerate([2023, 2024, 2025]):
        sub = d[d.ano == ano].set_index("senioridade")
        xs, vs = [], []
        for j, n in enumerate(niveis):
            if n in sub.index:
                xs.append(j + (i - 1) * w); vs.append(sub.loc[n, "salario_medio"])
        ax.bar(xs, vs, width=w - 0.03, color=COR_ANO[ano], label=str(ano))
        if ano == 2025:
            for x, v in zip(xs, vs):
                ax.text(x, v + 300, _brl(v), ha="center", fontsize=12.5, color=TXT, fontweight=600)
    ax.set_xticks(range(len(niveis)))
    ax.set_xticklabels(["Júnior", "Pleno", "Sênior", "Especialista\n(só 2025)", "Gestor"], fontsize=14, color=TXT)
    ax.set_yticks([]); ax.tick_params(axis="x", length=0)
    ax.legend(loc="upper left", ncol=3, fontsize=13)
    ax.set_ylim(0, 24000)
    return _salvar(fig, pasta, "g03_salario_senioridade.png")


def g04_salario_cargo(t, pasta):
    d = (t["gold_cargos"].query("ano == 2025 and qtd >= 30 and cargo != 'Outra Opção'")
         .nlargest(9, "salario_medio"))
    fig, ax = plt.subplots(figsize=(10, 5.8))
    cores = [AZUL if i < 2 else CINZA for i in range(len(d))]
    _barras_h(ax, [f"{NOMES_CARGO.get(c, c)}  ({p:.0f}% do mercado)"
                   for c, p in zip(d.cargo, d.pct_do_ano)],
              d.salario_medio.tolist(), cores, _brl)
    return _salvar(fig, pasta, "g04_salario_cargo_2025.png")


def g05_mulheres(t, pasta):
    g = t["gold_genero"].query("ano == 2025 and senioridade.notna()", engine="python")
    tot = g.groupby("senioridade").qtd.sum()
    fem = g[g.genero == "Feminino"].groupby("senioridade").qtd.sum()
    ordem = ["Júnior", "Pleno", "Sênior", "Especialista/Staff+", "Gestor"]
    pct = (fem / tot * 100).reindex(ordem)
    fig, ax = plt.subplots(figsize=(10, 5.4))
    ax.bar(range(5), pct.values, color=LARANJA, width=0.58)
    for i, v in enumerate(pct.values):
        ax.text(i, v + 0.6, _pct(v), ha="center", fontsize=15, fontweight=600)
    ax.set_xticks(range(5)); ax.set_xticklabels(["Júnior", "Pleno", "Sênior", "Especialista", "Gestor"], fontsize=14, color=TXT)
    ax.set_yticks([]); ax.tick_params(axis="x", length=0); ax.set_ylim(0, 34)
    return _salvar(fig, pasta, "g05_mulheres_por_nivel_2025.png")


def g06_gap_salarial(t, pasta):
    g = t["gold_genero"].query("ano == 2025 and senioridade.notna()", engine="python")
    p = g.pivot_table(index="senioridade", columns="genero", values="salario_medio")
    ordem = ["Júnior", "Pleno", "Sênior", "Especialista/Staff+", "Gestor"]
    p = p.reindex(ordem)
    fig, ax = plt.subplots(figsize=(11, 5.6))
    w = 0.36
    x = range(len(ordem))
    ax.bar([i - w/2 for i in x], p["Masculino"], width=w - 0.03, color=AZUL, label="Homens")
    ax.bar([i + w/2 for i in x], p["Feminino"], width=w - 0.03, color=LARANJA, label="Mulheres")
    for i, n in enumerate(ordem):
        gap = (p.loc[n, "Masculino"] - p.loc[n, "Feminino"]) / p.loc[n, "Masculino"] * 100
        txt = f"−{gap:.0f}%" if gap > 1 else "≈ igual"
        ax.text(i, max(p.loc[n]) + 700, txt, ha="center", fontsize=14, fontweight=600,
                color=TXT if gap > 1 else TXT2)
    ax.set_xticks(list(x)); ax.set_xticklabels(["Júnior", "Pleno", "Sênior", "Especialista", "Gestor"], fontsize=14, color=TXT)
    ax.set_yticks([]); ax.tick_params(axis="x", length=0); ax.set_ylim(0, 25000)
    ax.legend(loc="upper left", ncol=2, fontsize=13)
    return _salvar(fig, pasta, "g06_gap_salarial_2025.png")


def _linhas(ax, series, anos=(2023, 2024, 2025), ymax=100, ajuste_inicio=None):
    """series: lista de (rotulo, [v23, v24, v25], cor). Rótulo direto no fim.
    ajuste_inicio: deslocamento vertical do rótulo inicial, para evitar sobreposição."""
    ajuste_inicio = ajuste_inicio or [0] * len(series)
    for (rot, vals, cor), dy in zip(series, ajuste_inicio):
        ax.plot(anos, vals, color=cor, linewidth=3, marker="o", markersize=9,
                markeredgecolor="white", markeredgewidth=2)
        ax.text(anos[0] - 0.06, vals[0] + dy, _pct(vals[0]), ha="right", va="center", fontsize=13, color=TXT2)
        ax.text(anos[-1] + 0.06, vals[-1], f"{_pct(vals[-1])}  {rot}", ha="left", va="center",
                fontsize=14, color=TXT, fontweight=600)
    ax.set_xticks(anos); ax.set_xticklabels([str(a) for a in anos], fontsize=14, color=TXT)
    ax.set_xlim(anos[0] - 0.35, anos[-1] + 1.0); ax.set_ylim(0, ymax)
    ax.set_yticks([]); ax.tick_params(axis="x", length=0)
    ax.spines["bottom"].set_color(GRADE)


def g07_cloud(t, pasta):
    d = t["gold_tecnologias"].query("categoria == 'Cloud'")
    s = lambda n: d[d.tecnologia == n].sort_values("ano").pct_uso.tolist()
    fig, ax = plt.subplots(figsize=(10, 5.4))
    _linhas(ax, [("AWS", s("AWS"), AZUL), ("Azure", s("Azure"), LARANJA), ("GCP", s("GCP"), VERDE_AGUA)], ymax=60,
           ajuste_inicio=[-2.2, 0, 2.2])
    return _salvar(fig, pasta, "g07_cloud_evolucao.png")


def g08_stack_2025(t, pasta):
    d = t["gold_tecnologias"].query("ano == 2025 and tecnologia in ['Python','SQL','Power BI','AWS','Azure','GCP','Looker','R','Tableau']")
    d = d.sort_values("pct_uso", ascending=False)
    cor = {"Linguagem": AZUL, "Cloud": LARANJA, "BI": VERDE_AGUA}
    fig, ax = plt.subplots(figsize=(10, 5.8))
    _barras_h(ax, d.tecnologia.tolist(), d.pct_uso.tolist(), [cor[c] for c in d.categoria], _pct, maxv=100)
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=v, label=k) for k, v in cor.items()], loc="lower right", fontsize=13)
    return _salvar(fig, pasta, "g08_tecnologias_2025.png")


def g09_ia_uso(t, pasta):
    d = t["gold_ia_uso"].sort_values("ano")
    fig, ax = plt.subplots(figsize=(10, 5.4))
    _linhas(ax, [("usam IA generativa", d.pct_usa_ia.tolist(), AZUL),
                 ("empresa paga a ferramenta", d.pct_empresa_paga.tolist(), LARANJA),
                 ("usam só versão gratuita", d.pct_so_gratuita.tolist(), VERDE_AGUA)], ymax=105)
    ax.set_xlim(2022.65, 2026.6)
    return _salvar(fig, pasta, "g09_ia_uso.png")


def g10_ia_empresas(t, pasta):
    d = t["gold_ia_empresas"]
    prio = d[d.prioridade.isin(["Principal prioridade", "Entre as prioridades"])].groupby("ano").pct_do_ano.sum()
    nao = d[d.prioridade == "Não é prioridade"].set_index("ano").pct_do_ano
    fig, ax = plt.subplots(figsize=(10, 5.4))
    _linhas(ax, [("é prioridade na empresa", prio.sort_index().tolist(), AZUL),
                 ("não é prioridade", nao.sort_index().tolist(), CINZA)], ymax=75)
    return _salvar(fig, pasta, "g10_ia_prioridade_empresas.png")


def g11_regiao(t, pasta):
    d = t["gold_regiao"].query("ano == 2025").sort_values("qtd", ascending=False)
    fig, ax = plt.subplots(figsize=(10, 5.2))
    cores = [AZUL if r == "Sudeste" else CINZA for r in d.regiao]
    _barras_h(ax, [f"{r}  ·  {_pct(p)} dos profissionais" for r, p in zip(d.regiao, d.pct_do_ano)],
              d.salario_medio.tolist(), cores, _brl)
    return _salvar(fig, pasta, "g11_regiao_2025.png")


def g12_modelo(t, pasta):
    d = t["gold_modelo_trabalho"].query("ano == 2025")
    ordem = ["Remoto", "Híbrido flexível", "Híbrido fixo", "Presencial"]
    at = d[d.tipo == "Atual"].set_index("modelo").pct_do_ano.reindex(ordem)
    idl = d[d.tipo == "Ideal"].set_index("modelo").pct_do_ano.reindex(ordem)
    fig, ax = plt.subplots(figsize=(11, 5.4))
    w = 0.36
    x = range(4)
    ax.bar([i - w/2 for i in x], at, width=w - 0.03, color=CINZA, label="Como trabalha hoje")
    ax.bar([i + w/2 for i in x], idl, width=w - 0.03, color=AZUL, label="Como gostaria de trabalhar")
    for i in x:
        ax.text(i - w/2, at.iloc[i] + 0.8, _pct(at.iloc[i]), ha="center", fontsize=13, color=TXT2)
        ax.text(i + w/2, idl.iloc[i] + 0.8, _pct(idl.iloc[i]), ha="center", fontsize=14, fontweight=600)
    ax.set_xticks(list(x)); ax.set_xticklabels(ordem, fontsize=14, color=TXT)
    ax.set_yticks([]); ax.tick_params(axis="x", length=0); ax.set_ylim(0, 52)
    ax.legend(loc="upper right", fontsize=13)
    return _salvar(fig, pasta, "g12_modelo_trabalho_2025.png")


def g13_satisfacao(satisf, pasta):
    d = satisf.set_index("modelo_trabalho_atual").reindex(["Híbrido flexível", "Remoto", "Híbrido fixo", "Presencial"])
    fig, ax = plt.subplots(figsize=(10, 4.6))
    cores = [LARANJA if m == "Presencial" else CINZA for m in d.index]
    _barras_h(ax, d.index.tolist(), d.pct_satisfeitos.tolist(), cores, _pct, maxv=100)
    return _salvar(fig, pasta, "g13_satisfacao_modelo_2025.png")


def gerar_todos(t, extras, pasta="graficos"):
    """t: tabelas gold; extras: {'setores': df, 'satisfacao': df} vindos da Silver."""
    os.makedirs(pasta, exist_ok=True)
    return [
        g01_cargos(t, pasta), g02_setores(extras["setores"], pasta),
        g03_salario_senioridade(t, pasta), g04_salario_cargo(t, pasta),
        g05_mulheres(t, pasta), g06_gap_salarial(t, pasta),
        g07_cloud(t, pasta), g08_stack_2025(t, pasta),
        g09_ia_uso(t, pasta), g10_ia_empresas(t, pasta),
        g11_regiao(t, pasta), g12_modelo(t, pasta), g13_satisfacao(extras["satisfacao"], pasta),
    ]
