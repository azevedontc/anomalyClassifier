#!/usr/bin/env python3
from __future__ import annotations
import argparse
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from src.utils.textnorm import normalize_text
from sklearn.neighbors import LocalOutlierFactor
from sklearn.decomposition import PCA

DEFAULT_RULE_WEIGHTS = {
    "r_preco_muito_acima": 40,
    "r_baixa_concorrencia": 25,
    "r_baixo_desconto": 20,
    "r_concentracao_fornecedor": 15,
}

FEATURES = ["preco_unit_adj","g_mean","g_med","g_q75","z_price","iqr_score",
            "desconto","n_propostas","ug_cnpj_freq"]

def _safe_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["preco_unit_est"] = _safe_num(df.get("vlr_est_unit"))
    df["preco_unit_adj"] = _safe_num(df.get("vlr_adj_unit"))
    df["desc_norm"] = df["descricao"].map(normalize_text)
    df["grupo_item"] = df["desc_norm"].astype("category").cat.codes

    g = df.groupby("grupo_item")["preco_unit_adj"]
    stats = g.agg(["count", "mean", "median", "std"]).rename(
        columns={"count": "g_n", "mean": "g_mean", "median": "g_med", "std": "g_std"}
    )
    q = g.quantile([0.25, 0.75]).unstack()
    stats["g_q25"] = q[0.25];
    stats["g_q75"] = q[0.75]
    stats["g_iqr"] = stats["g_q75"] - stats["g_q25"]

    # Modificando o join para evitar conflito de colunas
    df = df.join(stats, on="grupo_item", rsuffix="_group")  # Adiciona sufixo para as colunas do grupo

    df["z_price"] = (df["preco_unit_adj"] - df["g_mean_group"]) / df["g_std_group"].replace(0, np.nan)
    df["iqr_score"] = (df["preco_unit_adj"] - df["g_q75_group"]) / df["g_iqr_group"].replace(0, np.nan)
    df["desconto"] = 1 - (df["preco_unit_adj"] / df["preco_unit_est"])
    conc = df.groupby(["ug", "cnpj_vencedor"]).size().rename("ug_cnpj_freq")
    df = df.join(conc, on=["ug", "cnpj_vencedor"])
    return df


def _rule_flags(df: pd.DataFrame) -> pd.DataFrame:
    flags = pd.DataFrame(index=df.index)
    flags["r_preco_muito_acima"]       = (df["iqr_score"] > 1.5) | (df["z_price"] > 3)
    flags["r_baixa_concorrencia"]      = df["n_propostas"].fillna(2) <= 1
    flags["r_baixo_desconto"]          = df["desconto"].fillna(0) < 0.02
    flags["r_concentracao_fornecedor"] = df["ug_cnpj_freq"].fillna(0) > df["ug_cnpj_freq"].median()
    return flags

def _rules_breakdown(flags: pd.DataFrame, weights: dict[str,int]) -> pd.DataFrame:
    # contribuições absolutas de cada regra (0 ou peso)
    contribs = pd.DataFrame(
        {f"contrib_{k}": flags[k].astype(int) * w for k, w in weights.items()},
        index=flags.index
    )
    raw_sum = contribs.sum(axis=1)
    rules_score = 100 * raw_sum / sum(weights.values())
    return contribs, raw_sum, rules_score

def _iso_forest_score(df: pd.DataFrame) -> pd.Series:
    X = df[FEATURES].fillna(0).replace([np.inf,-np.inf],0).values
    X = StandardScaler().fit_transform(X)
    clf = IsolationForest(n_estimators=300, contamination="auto", random_state=42).fit(X)
    dfc = clf.decision_function(X)       # maior = mais normal
    s = (dfc.min() - dfc)                # inverter: maior = mais anômalo
    s = 100 * (s - s.min()) / (s.max() - s.min() + 1e-9)
    return pd.Series(s, index=df.index, name="if_score")


def _lof_score(df: pd.DataFrame, n_neighbors: int = 35) -> pd.Series:
    X = df[FEATURES].fillna(0).replace([np.inf,-np.inf],0).values
    X = StandardScaler().fit_transform(X)
    lof = LocalOutlierFactor(n_neighbors=n_neighbors, novelty=False, contamination="auto")
    # negative_outlier_factor_: valores mais negativos = mais outlier
    lof_fit = lof.fit_predict(X)  # para preencher internamente
    s = -lof.negative_outlier_factor_  # maior = mais anômalo
    s = 100 * (s - s.min()) / (s.max() - s.min() + 1e-9)
    return pd.Series(s, index=df.index, name="lof_score")

def _pca_score(df: pd.DataFrame, n_components: int = 5) -> pd.Series:
    X = df[FEATURES].fillna(0).replace([np.inf,-np.inf],0).values
    X = StandardScaler().fit_transform(X)
    pca = PCA(n_components=n_components, random_state=42)
    Xp = pca.fit_transform(X)
    Xr = pca.inverse_transform(Xp)
    resid = ((X - Xr) ** 2).sum(axis=1)  # erro de reconstrução
    s = 100 * (resid - resid.min()) / (resid.max() - resid.min() + 1e-9)
    return pd.Series(s, index=df.index, name="pca_score")


def _format_pct(x: float) -> str:
    try:
        return f"{100*x:.1f}%"
    except Exception:
        return "—"

def _format_money(x: float) -> str:
    if pd.isna(x): return "—"
    return f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

def _explain_row(row: pd.Series, weights: dict[str, int]) -> str:
    def pct(x):
        return "—" if pd.isna(x) else f"{x * 100:.1f}%"

    def money(x):
        if pd.isna(x): return "—"
        s = f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"

    preco = row.get("preco_unit_adj")
    med = row.get("g_med")
    desc = row.get("desconto")
    nprop = row.get("n_propostas")
    freq = row.get("ug_cnpj_freq")
    ifs = row.get("if_score")
    rsc = row.get("rules_score")

    linhas = []
    linhas.append("**Resumo do cálculo**")
    linhas.append(f"- Preço unitário adjudicado: **{money(preco)}**")
    linhas.append(f"- Mediana do grupo: **{money(med)}**")
    linhas.append(f"- Desconto: **{pct(desc)}**")
    if not pd.isna(nprop): linhas.append(f"- Nº de propostas no lote: **{int(nprop)}**")
    if not pd.isna(freq):  linhas.append(f"- Vitórias do vencedor na UG: **{int(freq)}**")

    # Regras acionadas
    regras = []
    for k, w in weights.items():
        if row.get(k):
            nome = {
                "r_preco_muito_acima": "Preço muito acima do padrão",
                "r_baixa_concorrencia": "Baixa concorrência",
                "r_baixo_desconto": "Desconto muito baixo",
                "r_concentracao_fornecedor": "Concentração de fornecedor na UG",
            }.get(k, k)
            regras.append(f"- {nome} (+{w} pts)")

    if regras:
        linhas.append("**Regras acionadas**:")
        linhas.extend(regras)
    else:
        linhas.append("**Regras acionadas**: nenhuma")

    linhas.append("")
    linhas.append("**Composição da nota**")
    linhas.append(f"- Nota do modelo: **{ifs:.1f}**")
    linhas.append(f"- Nota das regras: **{rsc:.1f}**")
    linhas.append(f"- **Score final = 0,6×modelo + 0,4×regras = {row.get('score'):.1f}**")

    return "\n".join(linhas)

def score(df: pd.DataFrame,
          weights: dict[str,int] | None = None,
          return_all_columns: bool = False,
          model: str = "iforest",        # "iforest" | "lof" | "pca" | "ensemble"
          w_model: float = 0.6,
          w_rules: float = 0.4) -> pd.DataFrame:

    w = weights or DEFAULT_RULE_WEIGHTS
    df = _build_features(df)
    flags = _rule_flags(df)

    contribs, raw_sum, rules_score = _rules_breakdown(flags, w)

    # escolher modelo
    if model == "iforest":
        model_score = _iso_forest_score(df)
    elif model == "lof":
        model_score = _lof_score(df)
    elif model == "pca":
        model_score = _pca_score(df)
    elif model == "ensemble":
        s_if = _iso_forest_score(df)
        s_lof = _lof_score(df)
        s_pca = _pca_score(df)
        model_score = (s_if + s_lof + s_pca) / 3
    else:
        raise ValueError("model deve ser: iforest | lof | pca | ensemble")

    final = (w_model*model_score + w_rules*rules_score).round(1)

    out = df.copy()
    out["model_used"] = model
    out["model_score"] = model_score
    out["if_score"] = out["model_score"] if model == "iforest" else np.nan
    out["rules_raw"] = raw_sum
    out["rules_score"] = rules_score
    out["score"] = final
    out = pd.concat([out, flags.astype(int), contribs], axis=1)

    # Reduzir colunas para o CSV final
    cols = ["processo", "ano", "modalidade", "ug", "nrlote", "numeroitem", "descricao", "qtd",
            "preco_unit_est", "preco_unit_adj", "g_med", "desconto", "n_propostas",
            "cnpj_vencedor", "fornecedor_vencedor",
            "model_used", "model_score", "rules_score", "score"]
    if return_all_columns:
        return out.sort_values("score", ascending=False)
    return out.sort_values("score", ascending=False)[[c for c in cols if c in out.columns]]

def main():
    ap = argparse.ArgumentParser(description="Scoring de anomalias a partir do arquivo merged (parquet ou csv).")
    ap.add_argument("--input", required=True, help="arquivo merged (.parquet ou .csv)")
    ap.add_argument("--out", required=True, help="CSV de saída com ranking")
    ap.add_argument("--out-all", help="(opcional) CSV com todas as colunas e explicações detalhadas")
    ap.add_argument("--model", default="iforest", choices=["iforest","lof","pca","ensemble"])
    ap.add_argument("--w-model", type=float, default=0.6)
    ap.add_argument("--w-rules", type=float, default=0.4)
    args = ap.parse_args()

    base = pd.read_parquet(args.input) if args.input.endswith(".parquet") else pd.read_csv(args.input)
    out = score(base, model=args.model, w_model=args.w_model, w_rules=args.w_rules)
    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"[ok] score salvo em: {args.out} (modelo={args.model})")

    if args.out_all:
        full = score(base, return_all_columns=True, model=args.model, w_model=args.w_model, w_rules=args.w_rules)
        full.to_csv(args.out_all, index=False, encoding="utf-8-sig")
        print(f"[ok] detalhado salvo em: {args.out_all}")


if __name__ == "__main__":
    main()
