import pandas as pd
from ..models.unsupervised import iso_forest_score
from ..models.rules import rule_flags, rules_score

def compose_scores(df: pd.DataFrame, w_if=0.6, w_rules=0.4) -> pd.DataFrame:
    if_score = iso_forest_score(df)
    flags = rule_flags(df)
    r_score = rules_score(flags)
    final = (w_if*if_score + w_rules*r_score)
    out = df.copy()
    out["score"] = final.round(1)
    out["motivos"] = flags.apply(lambda row: [k for k,v in row.items() if v], axis=1)
    return out.sort_values("score", ascending=False)
