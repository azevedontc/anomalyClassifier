import streamlit as st
import pandas as pd

st.set_page_config(page_title="Anomalias em Licitações - PR", layout="wide")
st.title("Monitor de Anomalias em Licitações - PR")

uploaded = st.file_uploader("Carregue o arquivo MERGED (parquet ou csv)", type=["parquet","csv"])

if uploaded:
    df = pd.read_parquet(uploaded) if uploaded.name.endswith(".parquet") else pd.read_csv(uploaded)
    from src.pipeline.score_from_merged import score
    sc = score(df)

    st.sidebar.subheader("Filtros")
    ug = st.sidebar.multiselect("Órgão/UG", sorted([x for x in sc["ug"].dropna().unique().tolist()]))
    if ug:
        sc = sc[sc["ug"].isin(ug)]
    mod = st.sidebar.multiselect("Modalidade", sorted([x for x in sc["modalidade"].dropna().unique().tolist()]))
    if mod:
        sc = sc[sc["modalidade"].isin(mod)]

    st.metric("Itens avaliados", len(sc))
    st.dataframe(sc, use_container_width=True, height=600)

    st.download_button("Baixar resultados (CSV)", sc.to_csv(index=False).encode("utf-8"),
                       "anomalias.csv", "text/csv")
else:
    st.info("1) Rode o merge (ano ou multi-ano) e 2) faça upload aqui do arquivo gerado.")
