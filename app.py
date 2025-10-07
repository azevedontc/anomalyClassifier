import io
import streamlit as st
import pandas as pd
import numpy as np

from src.data_ingest import load_all
from src.preprocess import build_silver, compute_kpis
from src.apriori_assoc import build_item_baskets, mine_rules
from src.tops import top_winners_losers

st.set_page_config(page_title='Anomaly Tracker (Lei 14.133/21 - Obras)', layout='wide')
st.title('Anomaly Tracker — Obras (Lei 14.133/2021) — PR 2023–2025')

st.markdown("""
Este painel aplica **critérios operacionais inspirados na Lei 14.133/2021** (foco em **obras**) para sinalizar anomalias por:
- **VT/Estimado** alto (possível sobrepreço) ou baixo (possível inexequibilidade);
- **Baixa competitividade** (poucos participantes).
Os limiares são editáveis na barra lateral e documentados no README.
""")

# ---------- Helpers ----------
def downloaders(df: pd.DataFrame, base_name: str, label_prefix: str = "Baixar"):
    """Botões de export CSV/Excel para um DataFrame."""
    if df is None or df.empty:
        return
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(f"{label_prefix} CSV — {base_name}", csv_bytes,
                       file_name=f"{base_name}.csv", mime="text/csv")
    xls_mem = io.BytesIO()
    with pd.ExcelWriter(xls_mem, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="dados")
    st.download_button(f"{label_prefix} Excel — {base_name}", xls_mem.getvalue(),
                       file_name=f"{base_name}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ---------- Sidebar ----------
with st.sidebar:
    st.header("Filtros & Parâmetros (Lei 14.133/21)")
    anos = st.multiselect("Anos", options=[2023, 2024, 2025], default=[2023, 2024, 2025])
    st.caption("Obs.: o pré-processamento já restringe para **obras** (serviços de engenharia).")

    st.markdown("#### Limiares (ajuste conforme a pesquisa de preços utilizada)")
    superfat_delta = st.slider("Sobrepreço: VT acima do estimado em…", 0.05, 0.60, 0.25, 0.01, format="%.2f")
    inexeq_delta   = st.slider("Inexequível: VT abaixo do estimado em…", 0.05, 0.60, 0.30, 0.01, format="%.2f")
    min_particip   = st.number_input("Baixa competitividade se participantes <", min_value=1, max_value=10, value=3, step=1)

    st.markdown("""
    <small>
    Sugestões: **+25%** como heurística de sobrepreço (em analogia ao art. 125 – acréscimos contratuais até 25%);
    **−30%** como sinal conservador de inexequibilidade (arts. 59–60).
    Ajuste segundo a metodologia do seu TCC e a fonte do estimado (ex.: SINAPI/SICRO).
    </small>
    """, unsafe_allow_html=True)

# ---------- Data ----------
data = load_all(years=anos)
silver = build_silver(
    data['licitacoes'], data['itens'], data['empresas'],
    superfat_delta=float(superfat_delta),
    inexeq_delta=float(inexeq_delta),
    min_particip=int(min_particip)
)

# Filtros adicionais
orgaos = sorted(silver['orgao'].dropna().unique().tolist())
modalidades = sorted(silver['modalidade'].dropna().unique().tolist())

c1, c2, c3 = st.columns(3)
with c1:
    org_sel = st.multiselect("Órgãos", options=orgaos, default=[])
with c2:
    mod_sel = st.multiselect("Modalidades", options=modalidades, default=[])
with c3:
    only_irreg = st.selectbox("Mostrar", options=["Todas", "Apenas irregulares"], index=0)

mask = pd.Series(True, index=silver.index)
if org_sel:
    mask &= silver['orgao'].isin(org_sel)
if mod_sel:
    mask &= silver['modalidade'].isin(mod_sel)
if only_irreg == "Apenas irregulares":
    mask &= silver['flag_irregular'] == 1

silver_f = silver[mask].copy()

# ---------- KPIs ----------
st.subheader('KPIs — Paraná (recorte atual)')
kpis = compute_kpis(silver_f)
k1, k2, k3, k4 = st.columns(4)
k1.metric('Total licitações (obras)', int(kpis['total_licitacoes'].iloc[0]))
k2.metric('% Irregularidade', f"{kpis['indice_irregularidade_%'].iloc[0]}%")
k3.metric('Montante total (R$)', f"{kpis['montante_total_brl'].iloc[0]:,.2f}")
k4.metric('Montante sobrepreço (R$)', f"{kpis['montante_sobrepreco_brl'].iloc[0]:,.2f}")

# ---------- Resumo + Preview ----------
st.markdown('---')
st.markdown("""
### Resumo do cálculo (14.133/21 - obras)
- **VT** = `valor_ou_desconto_homologado`; se ausente, `valor_estimado`.
- **VT/Estimado** = indicador principal.
- **Flags**:
  - `flag_superfaturado` se VT/Estimado > (1 + limiar de sobrepreço).
  - `flag_inexequivel` se VT/Estimado < (1 − limiar de inexequibilidade).
  - `flag_baixa_compet` se participantes < mínimo.
- **Flag geral**: 1 se qualquer flag acima for 1.
- **Explicabilidade por processo** mostra os motivos.
""")

st.markdown("### Pré-visualização do dataset (silver)")
nrows = st.slider("Linhas para visualizar:", 10, 5000, 200, 10)
st.write("**Colunas:**", list(silver_f.columns))
st.dataframe(silver_f.head(nrows))
downloaders(silver_f, "silver_recorte")

# ---------- Top 20 ----------
st.markdown('---')
st.subheader('Top 20 — Maiores razões VT / Estimado')
top = silver_f.sort_values('vt_vs_estimado', ascending=False).head(20)
st.dataframe(top[['numprocesso','ano','orgao','modalidade','valor_estimado','valor_total','vt_vs_estimado','flag_superfaturado','flag_inexequivel','flag_baixa_compet','flag_irregular']])
downloaders(top, "top20_vt_estimado")

# ---------- Lista de irregulares ----------
st.markdown('---')
st.subheader('Lista de irregulares (recorte atual)')
irregs = silver_f.query("flag_irregular == 1").sort_values(["vt_vs_estimado","valor_total"], ascending=False)
st.dataframe(irregs)
downloaders(irregs, "irregulares")

# ---------- Explicabilidade ----------
st.markdown('---')
st.subheader('Explicabilidade por processo')
procs = silver_f['numprocesso'].dropna().astype(str).unique().tolist()
sel = st.selectbox("Escolha um processo", options=procs)
if sel:
    row = silver_f[silver_f['numprocesso'].astype(str) == sel].iloc[0]
    vt = float(row['valor_total']) if pd.notna(row['valor_total']) else 0.0
    est = float(row['valor_estimado']) if pd.notna(row['valor_estimado']) else float('nan')
    ratio = float(row['vt_vs_estimado']) if pd.notna(row['vt_vs_estimado']) else float('nan')
    st.write(f"**Órgão**: {row['orgao']}  |  **Modalidade**: {row['modalidade']}")
    st.write(f"**VT** = R$ {vt:,.2f}  •  **Estimado** = R$ {est:,.2f}  •  **VT/Estimado** = {ratio:,.2f}")
    st.write(f"**Participantes**: {int(row['participantes'])}  •  **Vencedores**: {int(row['vencedores'])}  •  **Qtd. itens**: {int(row['qtd_itens'])}  •  **Flag**: {int(row['flag_irregular'])}")
    st.markdown("**Motivos:** " + (row['explicacao'] or '—'))

# ---------- Winners/Losers ----------
st.markdown('---')
st.subheader('TOP Winners e TOP Losers (por vitórias/participações de lote)')
tw, tl = top_winners_losers(data['empresas'])
c5, c6 = st.columns(2)
with c5:
    st.markdown("#### Top Winners")
    st.dataframe(tw)
    downloaders(tw, "top_winners")
with c6:
    st.markdown("#### Top Losers")
    st.dataframe(tl)
    downloaders(tl, "top_losers")

# ---------- Regras de associação ----------
st.markdown('---')
st.subheader("Associações entre itens (FP-Growth) — apenas nas irregulares do recorte")

with st.expander("Parâmetros (otimize memória)"):
    min_support = st.slider("min_support", 0.001, 0.2, 0.01, 0.001)
    min_conf    = st.slider("min_confidence", 0.1, 0.9, 0.3, 0.05)
    min_item_freq = st.number_input("Frequência mínima do item (em processos irregulares)", 1, 50, 5)
    top_n_items = st.number_input("Top-N itens mais comuns (0 = sem limite)", 0, 5000, 2000)
    max_len     = st.slider("Tamanho máximo do itemset (max_len)", 2, 4, 3)

baskets = build_item_baskets(data['itens'], silver_f)
rules = mine_rules(
    baskets, min_support=float(min_support), min_confidence=float(min_conf),
    min_item_freq=int(min_item_freq), top_n_items=int(top_n_items), max_len=int(max_len)
)

if rules.empty:
    st.info("Nenhuma regra encontrada com os filtros/parâmetros atuais.")
else:
    st.dataframe(rules)
    downloaders(rules, "regras_associacao")
