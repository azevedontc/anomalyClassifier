import json
import pandas as pd
import numpy as np
import streamlit as st
import matplotlib.pyplot as plt

st.set_page_config(page_title="Detecção de Anomalias em Licitações", layout="wide")

st.title("Detecção de Anomalias em Licitações do Estado do PR (Construção/Obras)")

st.sidebar.header("Arquivos de entrada")
anom_file = st.sidebar.file_uploader("CSV de Anomalias (obrigatório)", type=["csv"])
merged_file = st.sidebar.file_uploader("CSV/Parquet Merged (opcional para explicação mais precisa)", type=["csv","parquet"])
meta_file = st.sidebar.file_uploader("Metadados (modelmeta-ANO.json)", type=["json"])

if anom_file is not None:
    df_anom = pd.read_csv(anom_file)
    st.subheader("Resumo")
    total = len(df_anom)
    anom = int(df_anom.get("is_anomalous", pd.Series([0]*total)).sum())
    col1, col2, col3 = st.columns(3)
    col1.metric("Registros", f"{total:,}".replace(",", "."))
    col2.metric("Anômalos", f"{anom:,}".replace(",", "."))
    col3.metric("Taxa", f"{(anom/max(total,1))*100:.1f}%")

    st.subheader("Resultados")
    st.dataframe(df_anom, use_container_width=True, height=420)

    st.markdown("---")
    st.subheader("Explicação de uma licitação")
    st.caption("Selecione um ID e visualize os maiores desvios (IQR). A explicação não é salva no CSV — é gerada aqui sob demanda.")

    # Se meta + merged disponíveis, melhor explicação
    meta = None
    df_merged = None
    if meta_file is not None:
        try:
            meta = json.load(meta_file)
        except Exception:
            st.warning("Não foi possível ler o JSON de metadados.")
    if merged_file is not None:
        try:
            if merged_file.name.endswith(".parquet"):
                df_merged = pd.read_parquet(merged_file)
            else:
                df_merged = pd.read_csv(merged_file)
        except Exception:
            st.warning("Não foi possível ler o arquivo merged.")

    # escolher id
    id_col = None
    # Tenta identificar coluna id
    for c in ["numprocesso","id_licitacao","id_da_licitacao","co_licitacao","cod_licitacao","cd_licitacao","nu_licitacao","numero_licitacao","num_licitacao","processo_licitatorio","id_processo","cod_processo","licitacao_id"]:
        if c in df_anom.columns:
            id_col = c; break
    if id_col is None:
        id_col = st.selectbox("Selecione a coluna de ID", df_anom.columns)

    sel_id = st.selectbox("Escolha o ID da licitação", df_anom[id_col].astype(str).unique())

    if st.button("Gerar explicação"):
        if df_merged is None or meta is None:
            st.warning("Para calcular a explicação, envie também o arquivo *Merged* e o *modelmeta-ANO.json*.")
        else:
            # localizar linha
            try:
                row_merged = df_merged[df_merged[id_col].astype(str)==str(sel_id)]
            except Exception:
                row_merged = pd.DataFrame()
            if row_merged.empty:
                st.error("ID não encontrado no arquivo merged.")
            else:
                feats = meta["features_used"]
                med = pd.Series(meta["medians"])
                iqr = pd.Series(meta["iqr"]).replace(0, np.nan)
                row_vals = row_merged[feats].copy()
                # imputação simples com mediana/meta
                row_vals = row_vals.fillna(med)
                # z-robusto
                zrob = (row_vals.iloc[0] - med) / iqr
                zrob = zrob.replace([np.inf, -np.inf], np.nan)
                contribs = zrob.abs().sort_values(ascending=False).head(10).dropna()
                st.write("**Top desvios (em IQR):**")

                # Tabela
                tab = pd.DataFrame({
                    "feature": contribs.index,
                    "zrob_abs": contribs.values,
                    "direcao": ["acima" if zrob[f] > 0 else "abaixo" for f in contribs.index],
                    "valor": [row_vals.iloc[0][f] for f in contribs.index]
                })
                st.dataframe(tab, use_container_width=True, height=300)

                # Gráfico de barras
                fig, ax = plt.subplots()
                ax.bar(contribs.index, contribs.values)
                ax.set_ylabel("Desvio robusto (|IQR|)")
                ax.set_xticklabels(contribs.index, rotation=45, ha="right")
                st.pyplot(fig)

else:
    st.info("Carregue ao menos o CSV de anomalias para começar.")
