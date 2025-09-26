import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules

def build_item_baskets(itens_df: pd.DataFrame, lic_silver: pd.DataFrame) -> pd.DataFrame:
    if itens_df.empty or lic_silver.empty:
        return pd.DataFrame()
    ilegais = lic_silver.query('flag_irregular == 1')[['numprocesso']].drop_duplicates()
    it = itens_df.merge(ilegais, how='inner', on='numprocesso')
    it['produto'] = it['item'].astype(str).str.strip().str.lower()
    baskets = it.groupby('numprocesso')['produto'].apply(lambda s: list(set(s))).reset_index()
    return baskets

def apriori_rules(baskets_df: pd.DataFrame, min_support=0.02, min_confidence=0.3) -> pd.DataFrame:
    if baskets_df.empty:
        return pd.DataFrame()
    all_items = sorted({p for prods in baskets_df['produto'] for p in prods})
    encoded = pd.DataFrame(0, index=baskets_df['numprocesso'], columns=all_items)
    for _, row in baskets_df.iterrows():
        encoded.loc[row['numprocesso'], row['produto']] = 1
    freq = apriori(encoded, min_support=min_support, use_colnames=True)
    if freq.empty:
        return pd.DataFrame()
    rules = association_rules(freq, metric='confidence', min_threshold=min_confidence)
    rules = rules.sort_values(['lift','confidence','support'], ascending=False)
    out = rules[['antecedents','consequents','support','confidence','lift']].copy()
    out['antecedents'] = out['antecedents'].apply(lambda s: ', '.join(sorted(list(s))))
    out['consequents'] = out['consequents'].apply(lambda s: ', '.join(sorted(list(s))))
    return out.reset_index(drop=True)
