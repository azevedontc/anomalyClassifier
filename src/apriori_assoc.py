import pandas as pd
from typing import List, Tuple
from mlxtend.frequent_patterns import association_rules, fpgrowth
from mlxtend.preprocessing import TransactionEncoder

def build_item_baskets(itens_df: pd.DataFrame, lic_silver: pd.DataFrame) -> pd.DataFrame:
    """Cestas por processo (apenas licitações marcadas como irregulares)."""
    if itens_df.empty or lic_silver.empty:
        return pd.DataFrame()

    ilegais = lic_silver.query("flag_irregular == 1")[['numprocesso']].drop_duplicates()
    it = itens_df.merge(ilegais, how='inner', on='numprocesso')

    it['produto'] = it['item'].astype(str).str.strip().str.lower()
    baskets = it.groupby('numprocesso')['produto'].apply(lambda s: list(set(s))).reset_index()
    return baskets

def _filter_items(baskets: pd.Series, min_item_freq: int, top_n_items: int | None) -> Tuple[List[List[str]], list[str]]:
    """Filtra itens raros e limita o vocabulário (Top N)."""
    # contagem de itens
    from collections import Counter
    c = Counter()
    for prods in baskets:
        c.update(set(prods))
    # aplica frequência mínima
    freq_ok = {k for k, v in c.items() if v >= min_item_freq}
    # restringe Top N (opcional)
    if top_n_items and top_n_items > 0:
        freq_ok = set(sorted(freq_ok, key=lambda k: c[k], reverse=True)[:top_n_items])
    # gera transações filtradas
    tx = [[p for p in prods if p in freq_ok] for prods in baskets]
    vocab = sorted(freq_ok)
    return tx, vocab

def mine_rules(
    baskets_df: pd.DataFrame,
    min_support: float = 0.02,
    min_confidence: float = 0.3,
    min_item_freq: int = 5,
    top_n_items: int | None = 2000,
    max_len: int = 3
) -> pd.DataFrame:
    """
    Mineração com FP-Growth (mais leve que Apriori).
    Estratégias anti-OOM:
      - filtra itens raros (min_item_freq)
      - limita vocabulário (top_n_items)
      - codificação booleana com TransactionEncoder
      - limita tamanho de itemset (max_len)
    """
    if baskets_df.empty:
        return pd.DataFrame()

    tx, vocab = _filter_items(baskets_df['produto'], min_item_freq=min_item_freq, top_n_items=top_n_items)
    if not vocab:
        return pd.DataFrame()

    te = TransactionEncoder()
    te.fit(tx)
    # matriz booleana (sem objetos string gigantes)
    arr = te.transform(tx)
    df_enc = pd.DataFrame(arr, columns=te.columns_).astype(bool)

    # FP-Growth é mais eficiente em memória que Apriori
    freq = fpgrowth(df_enc, min_support=min_support, use_colnames=True, max_len=max_len)
    if freq.empty:
        return pd.DataFrame()

    rules = association_rules(freq, metric="confidence", min_threshold=min_confidence)
    if rules.empty:
        return pd.DataFrame()

    rules = rules.sort_values(['lift', 'confidence', 'support'], ascending=False)
    out = rules[['antecedents','consequents','support','confidence','lift']].copy()
    out['antecedents'] = out['antecedents'].apply(lambda s: ", ".join(sorted(list(s))))
    out['consequents'] = out['consequents'].apply(lambda s: ", ".join(sorted(list(s))))
    return out.reset_index(drop=True)
