import streamlit as st
import pandas as pd
import numpy as np
from src.data_ingest import load_all
from src.preprocess import build_silver, compute_kpis
from src.apriori_assoc import build_item_baskets, mine_rules
from src.tops import top_winners_losers

import io

def downloaders(df: pd.DataFrame, base_name: str, label_prefix: str = "Baixar"):
    """Mostra botões para exportar CSV e Excel de um DataFrame."""
    if df is None or df.empty:
        return
    # CSV
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(f"{label_prefix} CSV — {base_name}", csv_bytes,
                       file_name=f"{base_name}.csv", mime="text/csv")
    # Excel
    xls_mem = io.BytesIO()
    with pd.ExcelWriter(xls_mem, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="dados")
    st.download_button(f"{label_prefix} Excel — {base_name}", xls_mem.getvalue(),
                       file_name=f"{base_name}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.set_page_config(page_title='Anomaly Tracker', layout='wide')
st.title('Anomaly Tracker - Detector de Anomalias em Licitações do PR')
st.markdown('''Este prototipo faz busca de licitacoes anomalas''')
with st.sidebar:
    st.header('Filtros')
    anos = st.multiselect('Anos', options=[2023,2024,2025], default=[2023,2024,2025])
    regime = st.selectbox('Regime legal (referencia LSL)', options=['8666','14133'], index=0)

data = load_all(years=anos)
silver = build_silver(data['licitacoes'], data['itens'], data['empresas'], regime=regime)

# filtros adicionais na UI
orgaos = sorted(silver['orgao'].dropna().unique().tolist())
modalidades = sorted(silver['modalidade'].dropna().unique().tolist())
tipos = sorted(silver['tipo'].dropna().unique().tolist())

c1,c2,c3,c4 = st.columns(4)
with c1:
    org_sel = st.multiselect('Orgaos', options=orgaos, default=[])
with c2:
    mod_sel = st.multiselect('Modalidades', options=modalidades, default=[])
with c3:
    tipo_sel = st.multiselect('Tipo', options=tipos, default=[])
with c4:
    flag_sel = st.selectbox('Somente irregular?', options=['Todos','Apenas irregular'], index=0)

mask = pd.Series(True, index=silver.index)
if org_sel:
    mask &= silver['orgao'].isin(org_sel)
if mod_sel:
    mask &= silver['modalidade'].isin(mod_sel)
if tipo_sel:
    mask &= silver['tipo'].isin(tipo_sel)
if flag_sel == 'Apenas irregular':
    mask &= silver['flag_irregular'] == 1
silver_f = silver[mask].copy()

st.subheader('KPIs — Paraná (recorte atual)')
kpis = compute_kpis(silver_f)
c1,c2,c3,c4 = st.columns(4)
c1.metric('Total licitacoes', int(kpis['total_licitacoes'].iloc[0]))
c2.metric('% Irregularidade', f"{kpis['indice_irregularidade_%'].iloc[0]}%")
c3.metric('Montante total (R$)', f"{kpis['montante_total_brl'].iloc[0]:,.2f}")
c4.metric('Excedente total (R$)', f"{kpis['excedente_total_brl'].iloc[0]:,.2f}")

st.markdown('---')
st.markdown('''
### Resumo do calculo (ranking)
- **Valor total (VT)**: `valor_ou_desconto_homologado`; se vazio, usa `valor_estimado`.
- **Tipo**: `obra_eng` vs `bens_servicos` por palavras-chave em `objeto`.
- **LSL**: limite superior de referencia por **regime** e **tipo** (arquivo `data/limits_br.csv`).
- **Razao**: **VT / LSL**; **Flag**: 1 se `VT > LSL`.
''')

st.markdown("---")
st.subheader("Explicabilidade por processo")
# lista dos processos do recorte
procs = silver_f['numprocesso'].dropna().astype(str).unique().tolist()
sel = st.selectbox("Escolha um processo", options=procs)

if sel:
    row = silver_f[silver_f['numprocesso'].astype(str) == sel].iloc[0]
    vt = float(row['valor_total']) if pd.notna(row['valor_total']) else 0.0
    lsl = float(row['lsl_cap']) if pd.notna(row['lsl_cap']) else float('nan')
    raz = float(row['razao_vt_lsl']) if pd.notna(row['razao_vt_lsl']) else float('nan')
    exc = max(vt - lsl, 0) if pd.notna(lsl) else float('nan')
    part = int(row['participantes'])
    venc = int(row['vencedores'])

    st.write(f"**Órgão**: {row['orgao']}  |  **Modalidade**: {row['modalidade']}  |  **Tipo**: {row['tipo']}")
    st.write(f"**VT** = R$ {vt:,.2f}  •  **LSL** = R$ {lsl:,.2f}  •  **VT/LSL** = {raz:,.2f}  •  **Excedente** = R$ {exc:,.2f}")
    st.write(f"**Participantes**: {part}  •  **Vencedores**: {venc}  •  **Qtd. Itens**: {int(row['qtd_itens'])}  •  **Flag**: {int(row['flag_irregular'])}")

    # sinais/insights simples
    sinais = []
    if pd.notna(raz):
        if raz >= 2: sinais.append("Razão VT/LSL muito alta (≥ 2,0).")
        elif raz > 1: sinais.append("VT acima do LSL (>1,0).")
    if part <= 2: sinais.append("Baixa concorrência (≤2 participantes).")
    if venc == 1 and part > 0 and (venc/part) > 0.8: sinais.append("Alta concentração de vitória no lote/processo.")
    if exc > 0: sinais.append(f"Excedente de R$ {exc:,.2f} sobre o LSL.")
    if not sinais: sinais.append("Sem sinais fortes — dentro de parâmetros informados para o LSL.")
    st.markdown("**Sinais detectados:**\n- " + "\n- ".join(sinais))


st.markdown('### Pre-visualizacao do dataset (silver)')
nrows = st.slider('Quantas linhas visualizar:', 10, 5000, 200, 10)
st.write('**Colunas do silver:**', list(silver_f.columns))
st.dataframe(silver_f.head(nrows))
st.download_button('Baixar silver (CSV)', silver_f.to_csv(index=False).encode('utf-8'), 'silver.csv', 'text/csv')

st.subheader("Top 20 — Maiores razoes VT / LSL")
top = silver_f.sort_values("razao_vt_lsl", ascending=False).head(20)
st.dataframe(top[['numprocesso','ano','orgao','modalidade','valor_total','lsl_cap','razao_vt_lsl','flag_irregular']])
downloaders(top, "top20_vt_lsl")

st.markdown("---")
st.subheader("Lista de irregulares (recorte atual)")
irregs = silver_f.query("flag_irregular == 1").sort_values(["razao_vt_lsl","valor_total"], ascending=False)
st.dataframe(irregs)
downloaders(irregs, "irregulares")

st.markdown('---')
st.subheader('TOP Winners e TOP Losers (por vitorias/participacoes de lote)')
tw, tl = top_winners_losers(data['empresas'])
c5, c6 = st.columns(2)
with c5:
    st.markdown('#### Top Winners')
    st.dataframe(tw)
with c6:
    st.markdown('#### Top Losers')
    st.dataframe(tl)

st.markdown('---')
st.subheader('Associacoes entre itens (Apriori) — somente nas licitacoes irregulares do recorte')
baskets = build_item_baskets(data['itens'], silver_f)
rules = mine_rules(baskets, min_support=0.01, min_confidence=0.3)
if rules.empty:
    st.info('Ainda nao ha regras geradas (ajuste filtros/anos).')
else:
    st.dataframe(rules)

with st.expander('Dicionario de dados (silver)'):
    dic = pd.DataFrame([
        ('numprocesso','Chave do processo'),
        ('ano','Ano da licitacao'),
        ('modalidade','Modalidade declarada'),
        ('situacao','Status do processo'),
        ('orgao','Orgao demandante'),
        ('objeto','Descricao do objeto'),
        ('tipo','Classificacao heuristica: obra_eng/bens_servicos'),
        ('valor_estimado','Valor estimado'),
        ('valor_ou_desconto_homologado','Valor homologado'),
        ('valor_total','VT = homologado ou estimado'),
        ('lsl_cap','Limite superior LSL conforme tabela e regime'),
        ('razao_vt_lsl','Razao VT/LSL'),
        ('flag_irregular','1 se VT > LSL'),
        ('participantes','Numero de empresas unicas participantes'),
        ('vencedores','Contagem de vencedores por processo'),
        ('qtd_itens','Quantidade de itens do processo'),
        ('excedente','Max(VT - LSL, 0)')
    ], columns=['coluna','descricao'])
    st.dataframe(dic)
