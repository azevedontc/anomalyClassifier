import re
from typing import List, Optional, Dict
import numpy as np
import pandas as pd
from .io_utils import read_csv_safely
from .config import KEYWORDS_OBRAS, MIN_FEATURE_COVERAGE, MIN_FEATURE_NUNIQUE

def find_columns(df: pd.DataFrame, patterns: List[str]) -> List[str]:
    cols = []
    for p in patterns:
        regex = re.compile(p)
        cols += [c for c in df.columns if regex.search(c)]
    return sorted(list(dict.fromkeys(cols)))

def try_parse_date(series: pd.Series) -> pd.Series:
    s = series.copy()
    s = s.str.replace(r"\s+\w+/\w+$", "", regex=True)
    return pd.to_datetime(s, errors="coerce", dayfirst=True, infer_datetime_format=True)

def to_numeric(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace(r"[^\d,.\-]", "", regex=True)
    if s.str.contains(",", regex=False).mean() > 0.4 and s.str.contains("\.", regex=False).mean() < 0.2:
        s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")

def detect_licitacao_key(df: pd.DataFrame) -> Optional[str]:
    priority = [
        "id_licitacao","id_da_licitacao","co_licitacao","cod_licitacao","cd_licitacao",
        "codigo_licitacao","nu_licitacao","numero_licitacao","num_licitacao","numprocesso",
        "processo_licitatorio","id_processo","cod_processo","licitacao_id"
    ]
    for p in priority:
        if p in df.columns:
            return p
    cands = [c for c in df.columns if re.search(r"licit|processo", c)]
    ranked = sorted(cands, key=lambda c: (not re.search(r"id|cod|nu|numero|cd|num", c), len(c)))
    return ranked[0] if ranked else None

def dedupe_columns(cols: List[str]) -> List[str]:
    seen = {}
    new_cols = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}__dup{seen[c]}")
    return new_cols

def multi_or_contains(text: str, keywords: List[str]) -> bool:
    import unicodedata
    t = unicodedata.normalize("NFKD", (text or "")).encode("ASCII", "ignore").decode().lower()
    return any(k in t for k in keywords)

def aggregate_by_licitacao(base_df: pd.DataFrame, others: Dict[str, pd.DataFrame], licit_key: str) -> pd.DataFrame:
    merged = base_df.copy()
    def add_agg_feature(name: str, feat_df: pd.DataFrame):
        nonlocal merged
        if licit_key not in feat_df.columns:
            from .features import detect_licitacao_key
            lk = detect_licitacao_key(feat_df)
            if not lk:
                return
            feat_df = feat_df.rename(columns={lk: licit_key})
        numeric_cols = []
        for c in feat_df.columns:
            if c == licit_key:
                continue
            if feat_df[c].dtype==object:
                if re.search(r"valor|vl\b|preco|quant|qtd|numero|num\b|porcent|percent|desconto|lote|item|particip|contrat|aditivo|alterac", c):
                    feat_df[c+"_num"] = to_numeric(feat_df[c])
                    numeric_cols.append(c+"_num")
            else:
                if np.issubdtype(feat_df[c].dtype, np.number):
                    numeric_cols.append(c)
        g = feat_df.groupby(licit_key, dropna=False)
        out = g.size().rename(f"{name}__qtd_registros").to_frame()
        for c in numeric_cols[:30]:
            out[f"{name}__sum_{c}"] = g[c].sum(min_count=1)
            out[f"{name}__mean_{c}"] = g[c].mean()
        out = out.reset_index()
        merged = merged.merge(out, on=licit_key, how="left")
    for t, df in others.items():
        add_agg_feature(t, df)
    return merged

def build_features(loaded: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    base_name = next((k for k in loaded if "licitac" in k), next(iter(loaded)))
    base_df = loaded[base_name].copy()

    licit_key = detect_licitacao_key(base_df) or "_licit_id"
    if licit_key not in base_df.columns:
        base_df[licit_key] = np.arange(len(base_df))

    text_cols = find_columns(base_df, [r"objeto", r"descricao", r"assunto", r"historico", r"categoria", r"natureza", r"segmento", r"grupo", r"modalidade", r"classe"])
    text_cols = list(dict.fromkeys(text_cols)) or [c for c in base_df.columns if base_df[c].dtype==object][:10]

    value_cols = find_columns(base_df, [r"\bvl\b", r"valor", r"preco", r"montante", r"estim", r"homolog", r"adjud", r"contrat"])
    date_cols  = find_columns(base_df, [r"data", r"dt_", r"_dt\b", r"\bdt\b", r"abert", r"public"])

    for c in value_cols:
        base_df[c + "__num"] = to_numeric(base_df[c]) if base_df[c].dtype==object else pd.to_numeric(base_df[c], errors="coerce")
    for c in date_cols:
        if base_df[c].dtype==object:
            base_df[c + "__date"] = try_parse_date(base_df[c])

    desc_col = next((c for c in ["objeto","descricao","descricao_objeto","descricao_resumida","assunto"] if c in base_df.columns), text_cols[0] if text_cols else None)
    org_cols = [c for c in base_df.columns if re.search(r"orgao|unidade|entidade|secretaria|municipio|cidade", c)]
    org_col = org_cols[0] if org_cols else None

    others = {k.replace("tb_",""):v for k,v in loaded.items() if k != base_name}
    merged = base_df[[licit_key] + ([desc_col] if desc_col else []) + ([org_col] if org_col else []) + value_cols + date_cols].copy()
    merged = aggregate_by_licitacao(merged, others, licit_key)
    merged.columns = dedupe_columns(list(merged.columns))

    # Filtro do nicho
    mask_any = False
    if desc_col and desc_col in merged.columns:
        mask_any = merged[desc_col].astype(str).apply(lambda x: multi_or_contains(x, KEYWORDS_OBRAS))
    else:
        mask_any = pd.Series([False]*len(merged), index=merged.index)
        for c in text_cols:
            if c in merged.columns:
                mask_any |= merged[c].astype(str).apply(lambda x: multi_or_contains(x, KEYWORDS_OBRAS))
    filtered = merged.loc[mask_any].copy()
    if len(filtered) == 0:
        filtered = merged.copy()
        filtered["_filtro_obras_aplicado"] = False
    else:
        filtered["_filtro_obras_aplicado"] = True

    # Features numéricas
    num_cols = [col for col in filtered.columns if col != licit_key and pd.api.types.is_numeric_dtype(filtered[col])]
    pub_cols = [c for c in filtered.columns if re.search(r"public", c) and c.endswith("__date")]
    abr_cols = [c for c in filtered.columns if re.search(r"abert", c) and c.endswith("__date")]
    if pub_cols and abr_cols:
        filtered["dias_ate_abertura"] = (filtered[abr_cols[0]] - filtered[pub_cols[0]]).dt.days
        num_cols.append("dias_ate_abertura")

    est_cols = [c for c in filtered.columns if re.search(r"estim", c) and c.endswith("__num")]
    hom_cols = [c for c in filtered.columns if re.search(r"homolog|adjud|contrat", c) and c.endswith("__num")]
    if est_cols:
        filtered["valor_estimado_total"] = filtered[est_cols].sum(axis=1, min_count=1)
        num_cols.append("valor_estimado_total")
    if hom_cols:
        filtered["valor_contratado_total"] = filtered[hom_cols].sum(axis=1, min_count=1)
        num_cols.append("valor_contratado_total")
    if est_cols and hom_cols:
        filtered["delta_contratado_estimado"] = filtered["valor_contratado_total"] - filtered["valor_estimado_total"]
        filtered["pct_sobrepreco_aparente"] = filtered["delta_contratado_estimado"] / filtered["valor_estimado_total"].replace(0,np.nan)
        num_cols += ["delta_contratado_estimado","pct_sobrepreco_aparente"]

    count_cols = [c for c in filtered.columns if c.endswith("__qtd_registros")]
    num_cols += count_cols

    # Seleção final
    X = filtered[num_cols].copy()
    valid_frac = X.notnull().mean()
    X = X.loc[:, valid_frac > MIN_FEATURE_COVERAGE]
    nunique = X.nunique(dropna=True)
    X = X.loc[:, nunique > MIN_FEATURE_NUNIQUE]
    feature_list = list(X.columns)
    return filtered, licit_key, desc_col, org_col, feature_list
