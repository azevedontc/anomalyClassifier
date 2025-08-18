from __future__ import annotations
from pathlib import Path
import pandas as pd

def _read_csv(path: Path) -> pd.DataFrame:
    # tenta encodings em ordem: UTF-8, UTF-8 com BOM, CP1252/Latin-1
    last_err = None
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        try:
            return pd.read_csv(path, sep=";", encoding=enc, dtype=str, low_memory=False)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Falha lendo {path.name} (encodings testados). Último erro: {last_err}")


def _coerce_keys(df: pd.DataFrame) -> pd.DataFrame:
    for c in ("numprocesso","ano","modalidade"):
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df

def merge_one_folder(folder: Path) -> pd.DataFrame:
    """
    Faz o merge para 1 ano. A pasta deve conter:
      TB_ITENS-ANO.csv, TB_LICITACOES-ANO.csv, TB_LOTES-ANO.csv,
      TB_EMPRESAS_PARTICIPANTES-ANO.csv, (opcional) TB_PUBLICIDADE-ANO.csv
    """
    year = folder.name[-4:]

    itens = _coerce_keys(_read_csv(folder / f"TB_ITENS-{year}.csv"))
    lic   = _coerce_keys(_read_csv(folder / f"TB_LICITACOES-{year}.csv"))
    lotes = _coerce_keys(_read_csv(folder / f"TB_LOTES-{year}.csv"))
    emp   = _coerce_keys(_read_csv(folder / f"TB_EMPRESAS_PARTICIPANTES-{year}.csv"))
    pub   = _read_csv(folder / f"TB_PUBLICIDADE-{year}.csv") if (folder / f"TB_PUBLICIDADE-{year}.csv").exists() else None
    if pub is not None:
        pub = _coerce_keys(pub)

    # ---- ITENS
    it = itens.rename(columns={
        "numprocesso":"processo","ano":"ano","modalidade":"modalidade","nrlote":"nrlote",
        "classeitem":"classeitem","numeroitem":"numeroitem","item":"descricao",
        "situacao":"situacao_item","quantidade":"qtd",
        "valor_estimado_unitario":"vlr_est_unit",
        "valor_ou_desconto_homologado_unitario":"vlr_adj_unit",
    })[["processo","ano","modalidade","nrlote","classeitem","numeroitem","descricao","situacao_item",
        "qtd","vlr_est_unit","vlr_adj_unit"]]

    # ---- LICITAÇÕES
    lic_slim = lic.rename(columns={
        "numprocesso":"processo","ano":"ano","modalidade":"modalidade",
        "situacao":"situacao_processo","objeto":"objeto","orgao":"ug",
        "dataabertura":"data_abertura","datahomologacao":"data_homologacao",
    })[["processo","ano","modalidade","situacao_processo","objeto","ug","data_abertura","data_homologacao"]]

    # ---- LOTES (vencedor do lote)
    lot = lotes.rename(columns={
        "numprocesso":"processo","ano":"ano","modalidade":"modalidade","nrlote":"nrlote",
        "cpf_cnpj_vencedor":"cnpj_vencedor","nome_razaosocial_vencedor":"fornecedor_vencedor",
        "valor_estimado":"vlr_est_lote","valor_ou_desconto_homologado":"vlr_adj_lote",
    })[["processo","ano","modalidade","nrlote","cnpj_vencedor","fornecedor_vencedor","vlr_est_lote","vlr_adj_lote"]]

    # ---- PARTICIPANTES → nº de propostas por lote
    emp_slim = emp.rename(columns={
        "numprocesso":"processo","ano":"ano","modalidade":"modalidade","nrlote":"nrlote",
        "cnpjcpfparticipante":"cnpj_participante",
    })[["processo","ano","modalidade","nrlote","cnpj_participante"]]
    n_prop = (emp_slim.groupby(["processo","ano","modalidade","nrlote"])["cnpj_participante"]
              .nunique().rename("n_propostas").reset_index())

    key_proc = ["processo","ano","modalidade"]
    key_lote = key_proc + ["nrlote"]

    df = (it.merge(lic_slim, on=key_proc, how="left")
            .merge(lot, on=key_lote, how="left")
            .merge(n_prop, on=key_lote, how="left"))

    # datas (aceitar yyyy-mm-dd e dd/mm/yyyy)
    for c in ("data_abertura","data_homologacao"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True)

    # publicidade (primeira data por processo/ano/modalidade)
    if pub is not None and "datapublicacao" in pub.columns:
        pub["datapublicacao"] = pd.to_datetime(pub["datapublicacao"], errors="coerce", dayfirst=True)
        pub_min = (pub.groupby(["numprocesso","ano","modalidade"])["datapublicacao"]
                   .min().rename("data_publicacao").reset_index()
                   .rename(columns={"numprocesso":"processo"}))
        df = df.merge(pub_min, on=key_proc, how="left")
        df["dias_pub_abrt"] = (df["data_abertura"] - df["data_publicacao"]).dt.days

    # numéricos
    for c in ("qtd","vlr_est_unit","vlr_adj_unit","vlr_est_lote","vlr_adj_lote","n_propostas"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # limpar quantidades inválidas
    df = df[df["qtd"].fillna(0) > 0].reset_index(drop=True)
    return df
