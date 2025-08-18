#!/usr/bin/env python3
from __future__ import annotations
import argparse
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from src.utils.textnorm import normalize_text

DEFAULT_RULE_WEIGHTS = {
    "r_preco_muito_acima": 40,
    "r_baixa_concorrencia": 25,
    "r_baixo_desconto": 20,
    "r_concentracao_fornecedor": 15,
}

def _safe_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # preços unitários (já vêm na base, mas normalizamos tipos)
    df["preco_unit_est"] = _safe_num(df.get("vlr_est_unit"))
    df["preco_unit_adj"] = _safe_num(df.get("vlr_adj_unit"))

    # normalização de texto e grupo
    df["desc_norm"] = df["descricao"].map(normalize_text)
    df["grupo_item"] = df["desc_norm"].astype("category").cat.codes

    # estatísticas por grupo
    g = df.groupby("grupo_item")["preco_unit_adj"]
    stats = g.agg(["count","mean","median","std"]).rename(columns={
        "count":"g_n","mean":"g_mean","median":"g_med","std":"g_std"
    })
    q = g.quantile([0.25,0.75]).unstack()
    stats["g_q25"] = q[0.25]; stats["g_q75"] = q[0.75]
    stats["g_iqr"] = stats["g_q75"] - stats["g_q25"]
    df = df.join(stats, on="grupo_item")

    # derivadas
    df["z_price"]   = (df["preco_unit_adj"] - df["g_mean"]) / df["g_std"].replace(0, np.nan)
    df["iqr_score"] = (df["preco_unit_adj"] - df["g_q75"]) / df["g_iqr"].replace(0, np.nan)
    df["desconto"]  = 1 - (df["preco_unit_adj"] / df["preco_unit_est"])
    conc = df.groupby(["ug","cnpj_vencedor"]).size().rename("ug_cnpj_freq")
    df = df.join(conc, on=["ug","cnpj_vencedor"])
    return df

def _rule_flags(df: pd.DataFrame) -> pd.DataFrame:
    flags = pd.DataFrame(index=df.index)
    flags["r_preco_muito_acima"]      = (df["iqr_score"] > 1.5) | (df["z_price"] > 3)
    flags["r_baixa_concorrencia"]     = df["n_propostas"].fillna(2) <= 1
    flags["r_baixo_desconto"]         = df["desconto"].fillna(0) < 0.02
    flags["r_concentracao_fornecedor"]= df["ug_cnpj_freq"].fillna(0) > df["ug_cnpj_freq"].median()
    return flags

def score(df: pd.DataFrame, weights: dict[str,int] | None = None) -> pd.DataFrame:
    df = _build_features(df)
    flags = _rule_flags(df)
    w = weights or DEFAULT_RULE_WEIGHTS

    # score de regras
    r_score = (flags.astype(int) * pd.Series(w)).sum(axis=1)
    r_score = 100 * r_score / sum(w.values())

    # Isolation Forest
    feats = ["preco_unit_adj","g_mean","g_med","g_q75","z_price","iqr_score","desconto","n_propostas","ug_cnpj_freq"]
    X = df[feats].fillna(0).replace([np.inf,-np.inf],0).values
    X = StandardScaler().fit_transform(X)
    clf = IsolationForest(n_estimators=300, contamination="auto", random_state=42).fit(X)
    dfc = clf.decision_function(X)
    if_score = (dfc.min() - dfc)
    if_score = 100 * (if_score - if_score.min()) / (if_score.max() - if_score.min() + 1e-9)

    df["score"] = (0.6*if_score + 0.4*r_score).round(1)
    df["motivos"] = flags.apply(lambda row: [k for k, v in row.items() if v], axis=1)

    cols = ["processo","ano","modalidade","ug","nrlote","numeroitem","descricao","qtd",
            "preco_unit_est","preco_unit_adj","g_med","desconto","n_propostas",
            "cnpj_vencedor","fornecedor_vencedor","score","motivos"]
    return df.sort_values("score", ascending=False)[[c for c in cols if c in df.columns]]

def main():
    ap = argparse.ArgumentParser(description="Scoring de anomalias a partir do arquivo merged (parquet ou csv).")
    ap.add_argument("--input", required=True, help="caminho do arquivo merged")
    ap.add_argument("--out", required=True, help="CSV de saída com ranking")
    args = ap.parse_args()

    if args.input.endswith(".parquet"):
        base = pd.read_parquet(args.input)
    else:
        base = pd.read_csv(args.input)

    out = score(base)
    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"[ok] score salvo em: {args.out}")
    print(out.head(10))

if __name__ == "__main__":
    main()
