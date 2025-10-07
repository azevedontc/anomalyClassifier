import pandas as pd

def top_winners_losers(empresas: pd.DataFrame):
    if empresas.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = empresas.copy()
    df['colocacao'] = df['colocacao'].astype(str)

    # unidade de competição: (numprocesso, nrlote)
    df['pair'] = df['numprocesso'].astype(str) + '::' + df['nrlote'].astype(str)

    part = df.groupby(['cnpjcpfparticipante','razaosocialnomeparticipante'])['pair'].nunique().reset_index(name='participacoes_lote')
    winners = df[df['colocacao'] == '1'].groupby(['cnpjcpfparticipante','razaosocialnomeparticipante'])['pair'].nunique().reset_index(name='vitorias_lote')

    res = part.merge(winners, how='left', on=['cnpjcpfparticipante','razaosocialnomeparticipante']).fillna({'vitorias_lote':0})
    res['vitorias_lote'] = res['vitorias_lote'].astype(int)
    res['taxa_vitoria_%'] = (res['vitorias_lote'] / res['participacoes_lote'] * 100).round(2)

    top_w = res.sort_values(['vitorias_lote','taxa_vitoria_%','participacoes_lote'], ascending=False).head(20)
    lose = res.sort_values(['vitorias_lote','participacoes_lote'], ascending=[True, False]).head(20)

    top_w = top_w.rename(columns={'vitorias_lote':'vitorias','participacoes_lote':'participacoes'})
    lose  = lose.rename(columns={'vitorias_lote':'vitorias','participacoes_lote':'participacoes'})
    return top_w, lose
