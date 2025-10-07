import pandas as pd
import numpy as np
from .utils_text import classify_tipo, normalize_text

"""
Pré-processamento com foco exclusivo na Lei 14.133/2021 (obras).

Ideia: abandonar LSL genérico e trabalhar com sinais previstos/derivados da 14.133:
- competitividade (art. 5º), 
- propostas inexequíveis/superfaturadas (arts. 59 e 60),
- justificativa de preços (art. 23, art. 18, art. 24; referência a painéis/bases oficiais).

Operationalizamos com limiares configuráveis:
  superfat_delta  -> VT > (1 + superfat_delta) * estimado  (ex.: +25%)
  inexeq_delta    -> VT < (1 - inexeq_delta) * estimado    (ex.: -30%)
  min_particip    -> baixa competitividade se participantes < min_particip (ex.: 3)

Obs.: “valor_estimado” deve refletir pesquisa de preços (ex.: SINAPI/SICRO ou painel de preços).
"""

def build_silver(
    licitacoes: pd.DataFrame,
    itens: pd.DataFrame,
    empresas: pd.DataFrame,
    *,
    superfat_delta: float = 0.25,   # +25% (art. 125: acréscimos contratuais até 25%) como heurística de sobrepreço
    inexeq_delta: float = 0.30,     # -30% (conservador) para proposta possivelmente inexequível
    min_particip: int = 3           # <3 participantes = baixa competitividade
) -> pd.DataFrame:
    lic = licitacoes.copy()

    # Garantir colunas básicas
    for col in ['numprocesso','ano','modalidade','objeto','orgao','situacao','valor_estimado','valor_ou_desconto_homologado']:
        if col not in lic.columns:
            lic[col] = np.nan

    lic['numprocesso'] = lic['numprocesso'].astype('string')

    # VT = homologado (preferência) ou estimado (fallback)
    vt_h = pd.to_numeric(lic['valor_ou_desconto_homologado'], errors='coerce')
    vt_e = pd.to_numeric(lic['valor_estimado'], errors='coerce')
    lic['valor_total'] = vt_h.fillna(vt_e)

    # Classificação do objeto e RECORTE: somente obras/serviços de engenharia
    lic['tipo'] = lic['objeto'].apply(classify_tipo)
    lic = lic[lic['tipo'] == 'obra_eng'].copy()

    # Participantes/vencedores por processo
    if empresas is not None and not empresas.empty:
        part = empresas[['numprocesso','nrlote','colocacao','razaosocialnomeparticipante']].copy()
        part['is_winner'] = (part['colocacao'].astype(str) == '1').astype(int)
        agg = part.groupby('numprocesso').agg(
            participantes=('razaosocialnomeparticipante','nunique'),
            vencedores=('is_winner','sum')
        ).reset_index()
        lic = lic.merge(agg, how='left', on='numprocesso')

    lic['participantes'] = lic.get('participantes', 0).fillna(0).astype(int)
    lic['vencedores'] = lic.get('vencedores', 0).fillna(0).astype(int)

    # Itens por processo
    if itens is not None and not itens.empty:
        itc = itens.groupby('numprocesso').size().reset_index(name='qtd_itens')
        lic = lic.merge(itc, how='left', on='numprocesso')
    else:
        lic['qtd_itens'] = 0

    # VT/Estimado e flags 14.133
    est = pd.to_numeric(lic['valor_estimado'], errors='coerce')
    lic['vt_vs_estimado'] = lic['valor_total'] / est
    lic['flag_superfaturado'] = (lic['vt_vs_estimado'] > (1 + superfat_delta)).astype(int)
    lic['flag_inexequivel']   = (lic['vt_vs_estimado'] < (1 - inexeq_delta)).astype(int)
    lic['flag_baixa_compet']  = (lic['participantes'] < min_particip).astype(int)
    lic['flag_irregular']     = ((lic['flag_superfaturado'] == 1) |
                                 (lic['flag_inexequivel'] == 1) |
                                 (lic['flag_baixa_compet'] == 1)).astype(int)

    # Explicação (legível)
    def _exp(row):
        msgs = []
        r = row.get('vt_vs_estimado')
        if pd.notna(r):
            if r > (1 + superfat_delta):
                msgs.append(f"VT/Estimado = {r:.2f} (> {1+superfat_delta:.2f}) — possível sobrepreço.")
            if r < (1 - inexeq_delta):
                msgs.append(f"VT/Estimado = {r:.2f} (< {1-inexeq_delta:.2f}) — possível inexequibilidade.")
        if row.get('participantes', 0) < min_particip:
            msgs.append(f"Baixa competitividade: {int(row.get('participantes',0))} participante(s) (< {min_particip}).")
        return " | ".join(msgs) if msgs else "Sem sinais fortes no critério adotado."

    lic['explicacao'] = lic.apply(_exp, axis=1)

    # Normalizações de texto úteis p/ busca
    lic['objeto_norm'] = lic['objeto'].astype(str).apply(normalize_text)
    lic['orgao_norm']  = lic['orgao'].astype(str).apply(normalize_text)

    keep = [
        'numprocesso','ano','modalidade','situacao','orgao','objeto','tipo',
        'valor_estimado','valor_ou_desconto_homologado','valor_total',
        'vt_vs_estimado','flag_superfaturado','flag_inexequivel','flag_baixa_compet',
        'flag_irregular','participantes','vencedores','qtd_itens','explicacao'
    ]
    silver = lic[keep].copy()
    return silver


def compute_kpis(silver: pd.DataFrame) -> pd.DataFrame:
    total_licit = len(silver)
    total_valor = pd.to_numeric(silver['valor_total'], errors='coerce').sum(skipna=True)
    irreg = int(pd.to_numeric(silver['flag_irregular'], errors='coerce').sum())
    idx_irregularidade = (irreg / total_licit * 100) if total_licit else 0.0

    # “prejuízo” operacional: soma de VT das sinalizadas por sobrepreço
    sobre = pd.to_numeric(silver.loc[silver['flag_superfaturado'] == 1, 'valor_total'], errors='coerce').sum(skipna=True)
    idx_prejuizo = (sobre / total_valor * 100) if total_valor else 0.0

    return pd.DataFrame({
        'total_licitacoes': [total_licit],
        'irregulares': [irreg],
        'indice_irregularidade_%': [round(idx_irregularidade, 2)],
        'montante_total_brl': [round(total_valor, 2)],
        'montante_sobrepreco_brl': [round(sobre, 2)],
        'indice_prejuizo_%': [round(idx_prejuizo, 2)],
    })
