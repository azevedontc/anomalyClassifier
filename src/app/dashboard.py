import streamlit as st
import pandas as pd

st.set_page_config(page_title="Anomalias em Licitações - PR", layout="wide")
st.title("Monitor de Anomalias em Licitações - PR")

uploaded = st.file_uploader("Carregue o arquivo MERGED (parquet ou csv)", type=["parquet","csv"])

if uploaded:
    df = pd.read_parquet(uploaded) if uploaded.name.endswith(".parquet") else pd.read_csv(uploaded)
    from src.pipeline.score_from_merged import score
    sc_full = score(df, return_all_columns=True)  # já traz if_score/rules/explicacao

    st.sidebar.subheader("Filtros")
    ug = st.sidebar.multiselect("Órgão/UG", sorted(sc_full["ug"].dropna().unique().tolist()))
    mod = st.sidebar.multiselect("Modalidade", sorted(sc_full["modalidade"].dropna().unique().tolist()))

    sc = sc_full.copy()
    if ug:  sc = sc[sc["ug"].isin(ug)]
    if mod: sc = sc[sc["modalidade"].isin(mod)]

    st.metric("Itens avaliados", len(sc))

    # índice estável para seleção
    sc = sc.reset_index(drop=True)
    st.dataframe(sc[[
        "processo","ano","modalidade","ug","nrlote","numeroitem","descricao",
        "preco_unit_adj","g_med","desconto","n_propostas","if_score","rules_score","score"
    ]], use_container_width=True, height=600)

    st.divider()
    st.subheader("Explicação da nota")

    idx = st.number_input("Escolha o índice da linha (acima, coluna à esquerda)", min_value=0, max_value=max(0, len(sc)-1), value=0, step=1)
    if len(sc) > 0:
        row = sc.iloc[int(idx)]
        st.write(f"**Processo {row.get('processo')} | Item {row.get('numeroitem','—')} | UG: {row.get('ug','—')}**")
        st.write(row.get("explicacao","—"))

        with st.expander("Detalhamento numérico"):
            cols_show = [
                "preco_unit_est","preco_unit_adj","g_med","z_price","iqr_score","desconto","n_propostas","ug_cnpj_freq",
                "if_score","rules_score","score",
                "r_preco_muito_acima","r_baixa_concorrencia","r_baixo_desconto","r_concentracao_fornecedor",
                "contrib_r_preco_muito_acima","contrib_r_baixa_concorrencia","contrib_r_baixo_desconto","contrib_r_concentracao_fornecedor",
            ]
            cols_show = [c for c in cols_show if c in sc.columns]
            st.dataframe(row[cols_show].to_frame("valor"))
else:
    st.info("1) Rode o merge (ano ou multi-ano) e 2) faça upload aqui do arquivo *merged*.")
