import numpy as np
import pandas as pd

def rule_flags(df: pd.DataFrame) -> pd.DataFrame:
    flags = pd.DataFrame(index=df.index)
    flags["r_preco_muito_acima"] = (df["iqr_score"] > 1.5) | (df["z_price"] > 3)  # ajuste fino depois
    flags["r_baixa_concorrencia"] = df["n_propostas"].fillna(2) <= 1
    flags["r_baixo_desconto"] = df["desconto"].fillna(0) < 0.02
    flags["r_concentracao_fornecedor"] = df["ug_cnpj_freq"].fillna(0) > df["ug_cnpj_freq"].median()
    return flags

def rules_score(flags: pd.DataFrame, weights=None) -> pd.Series:
    if weights is None:
        weights = {
            "r_preco_muito_acima": 40,
            "r_baixa_concorrencia": 25,
            "r_baixo_desconto": 20,
            "r_concentracao_fornecedor": 15
        }
    s = sum(flags[k].astype(int) * w for k,w in weights.items())
    # normalizar para 0-100
    return (100 * s / sum(weights.values()))
