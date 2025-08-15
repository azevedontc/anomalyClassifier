import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest

FEATURES = [
    "preco_unit_adj","g_mean","g_med","g_q75","z_price","iqr_score",
    "desconto","n_propostas","dias_pub_abrt","ug_cnpj_freq"
]

def iso_forest_score(df: pd.DataFrame, random_state=42) -> pd.Series:
    X = df[FEATURES].fillna(0).replace([np.inf,-np.inf],0).values
    X = StandardScaler().fit_transform(X)
    clf = IsolationForest(
        n_estimators=300, contamination="auto", random_state=random_state
    ).fit(X)
    # decision_function: maior = mais normal → converter para “anomalia alta = score alto”
    dfc = clf.decision_function(X)
    s = (dfc.min() - dfc)  # reverter
    # normaliza 0-100
    s = 100 * (s - s.min()) / (s.max() - s.min() + 1e-9)
    return pd.Series(s, index=df.index, name="if_score")
