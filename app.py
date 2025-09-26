import streamlit as st
import pandas as pd
import numpy as np
from src.data_ingest import load_all
from src.preprocess import build_silver, compute_kpis
from src.apriori_assoc import build_item_baskets, apriori_rules
from src.tops import top_winners_losers

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

st.markdown('### Pre-visualizacao do dataset (silver)')
nrows = st.slider('Quantas linhas visualizar:', 10, 5000, 200, 10)
st.write('**Colunas do silver:**', list(silver_f.columns))
st.dataframe(silver_f.head(nrows))
st.download_button('Baixar silver (CSV)', silver_f.to_csv(index=False).encode('utf-8'), 'silver.csv', 'text/csv')

st.markdown('---')
st.subheader('Top 20 — Maiores razoes VT / LSL')
top = silver_f.sort_values('razao_vt_lsl', ascending=False).head(20)
st.dataframe(top[['numprocesso','ano','orgao','modalidade','valor_total','lsl_cap','razao_vt_lsl','flag_irregular']])

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
rules = apriori_rules(baskets, min_support=0.01, min_confidence=0.3)
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
