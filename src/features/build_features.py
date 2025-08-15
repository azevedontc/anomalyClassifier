import pandas as pd
import numpy as np

def group_stats(df: pd.DataFrame, col_group="grupo_item", col_price="preco_unit_adj"):
    g = df.groupby(col_group)[col_price]
    stats = g.agg(["count","mean","median","std","min","max"]).rename(
        columns={"count":"g_n","mean":"g_mean","median":"g_med","std":"g_std","min":"g_min","max":"g_max"}
    )
    q = g.quantile([0.25,0.75]).unstack()
    stats["g_q25"] = q[0.25]; stats["g_q75"] = q[0.75]
    stats["g_iqr"] = stats["g_q75"] - stats["g_q25"]
    return df.join(stats, on=col_group)

def add_derived(df: pd.DataFrame) -> pd.DataFrame:
    df = group_stats(df)
    # Z-score e IQR score
    df["z_price"] = (df["preco_unit_adj"] - df["g_mean"]) / df["g_std"].replace(0, np.nan)
    df["iqr_score"] = (df["preco_unit_adj"] - df["g_q75"]) / df["g_iqr"].replace(0, np.nan)
    # Desconto
    df["desconto"] = 1 - (df["vlr_adj"] / df["vlr_est"])
    # Concentração por UG-fornecedor
    conc = df.groupby(["ug","cnpj"]).size().rename("ug_cnpj_freq")
    df = df.join(conc, on=["ug","cnpj"])
    return df
