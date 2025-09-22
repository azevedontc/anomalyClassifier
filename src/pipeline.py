import os, re, json, argparse
from typing import Dict
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

from .io_utils import read_csv_safely
from .features import build_features
from .config import DEFAULT_CONTAMINATION

def load_year(input_dir: str, year: str) -> Dict[str, pd.DataFrame]:
    files = [f for f in os.listdir(input_dir) if f.lower().endswith(".csv") and year in f]
    loaded = {}
    for f in sorted(files):
        df = read_csv_safely(os.path.join(input_dir, f))
        if df is not None:
            key = os.path.splitext(f)[0].lower()
            loaded[key] = df
    if not loaded:
        raise SystemExit(f"Nenhum CSV encontrado para o ano {year} em {input_dir}")
    return loaded

def run_year(input_dir: str, year: str, out_dir: str, contamination: float = DEFAULT_CONTAMINATION):
    os.makedirs(os.path.join(out_dir, year), exist_ok=True)
    out_y = os.path.join(out_dir, year)

    loaded = load_year(input_dir, str(year))
    filtered, licit_key, desc_col, org_col, feature_list = build_features(loaded)

    # Matriz de treino
    X = filtered[feature_list].copy()
    X_imp = X.fillna(X.median())

    # Escala + IF
    scaler = RobustScaler()
    X_scaled = scaler.fit_transform(X_imp.values)

    n = len(X_imp)
    contamination = contamination if n>=100 else max(0.02, min(0.15, 10.0/max(20,n)))
    iso = IsolationForest(
        n_estimators=400,
        contamination=contamination,
        random_state=42,
        bootstrap=True,
        n_jobs=-1
    )
    iso.fit(X_scaled)
    scores = -iso.decision_function(X_scaled)
    thresh = float(np.quantile(scores, 1.0 - contamination))
    labels = (scores >= thresh).astype(int)

    # Anexar resultados ao dataset filtrado
    final = filtered.copy()
    final["anomaly_score"] = scores
    final["is_anomalous"] = labels

    # Escrever saídas — SEM explicação textual dentro do CSV
    merged_csv  = os.path.join(out_y, f"licitacoes_construcao_obras_merged-{year}.csv")
    anom_csv    = os.path.join(out_y, f"licitacoes_construcao_obras_anomalias-{year}.csv")

    final.to_csv(merged_csv, index=False, encoding="utf-8")

    # Compacto para consumo no front
    keep_cols = [licit_key]
    for dc in ["objeto","descricao","descricao_objeto","descricao_resumida","assunto"]:
        if dc in final.columns:
            keep_cols.append(dc); break
    for oc in [c for c in final.columns if re.search(r"orgao|unidade|entidade|secretaria|municipio|cidade", c)]:
        keep_cols.append(oc); break
    for c in ["valor_estimado_total","valor_contratado_total","pct_sobrepreco_aparente","dias_ate_abertura"]:
        if c in final.columns: keep_cols.append(c)
    keep_cols += [c for c in final.columns if c.endswith("__qtd_registros")]
    keep_cols += ["anomaly_score","is_anomalous"]
    compact = final[keep_cols].copy()
    compact.to_csv(anom_csv, index=False, encoding="utf-8")

    # Metadados p/ front calcular explicações sob demanda
    med = pd.Series(X_imp.median(), index=X_imp.columns).to_dict()
    iqr = (X_imp.quantile(0.75) - X_imp.quantile(0.25)).replace(0, np.nan).to_dict()
    meta = {
        "year": year,
        "features_used": feature_list,
        "medians": med,
        "iqr": iqr,
        "contamination": float(contamination),
        "threshold": thresh,
        "licitacao_key": licit_key,
        "base_table": next((k for k in loaded if "licitac" in k), next(iter(loaded)))
    }
    meta_path = os.path.join(out_y, f"modelmeta-{year}.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return {"merged_csv": merged_csv, "anom_csv": anom_csv, "meta_json": meta_path, "rows": int(n), "anom_count": int(labels.sum())}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir", required=True)
    ap.add_argument("--year", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--contamination", type=float, default=DEFAULT_CONTAMINATION)
    args = ap.parse_args()
    info = run_year(args.input_dir, args.year, args.out_dir, contamination=args.contamination)
    print(json.dumps(info, ensure_ascii=False, indent=2))
