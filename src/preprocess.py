import pandas as pd
import numpy as np
from .utils_text import classify_tipo, normalize_text
from .legal_limits import LegalLimits
from .config import DEFAULT_REGIME

def build_silver(licitacoes: pd.DataFrame, itens: pd.DataFrame, empresas: pd.DataFrame, regime: str = DEFAULT_REGIME):
    lic = licitacoes.copy()

    for col in ['numprocesso','ano','modalidade','objeto','orgao','situacao','valor_estimado','valor_ou_desconto_homologado']:
        if col not in lic.columns:
            lic[col] = np.nan

    lic['numprocesso'] = lic['numprocesso'].astype('string')
    vt_h = pd.to_numeric(lic['valor_ou_desconto_homologado'], errors='coerce')
    vt_e = pd.to_numeric(lic['valor_estimado'], errors='coerce')
    lic['valor_total'] = vt_h.fillna(vt_e)
    lic['tipo'] = lic['objeto'].apply(classify_tipo)
    # participantes / vencedores por processo
    if not empresas.empty:
        part = empresas[['numprocesso','nrlote','colocacao','razaosocialnomeparticipante']].copy()
        part['numprocesso'] = part['numprocesso'].astype('string')
        part['is_winner'] = (part['colocacao'].astype(str) == '1').astype(int)
        agg = part.groupby('numprocesso').agg(
            participantes=('razaosocialnomeparticipante','nunique'),
            vencedores=('is_winner','sum')
        ).reset_index()
        lic = lic.merge(agg, how='left', on='numprocesso')
    lic['participantes'] = lic.get('participantes', 0).fillna(0).astype(int)
    lic['vencedores'] = lic.get('vencedores', 0).fillna(0).astype(int)
    # qtd_itens por processo
    if not itens.empty:
        itens = itens.copy()
        if 'numprocesso' not in itens.columns:
            itens['numprocesso'] = pd.Series(dtype='string')
        itens['numprocesso'] = itens['numprocesso'].astype('string')
        itc = itens.groupby('numprocesso').size().reset_index(name='qtd_itens')
        lic = lic.merge(itc, how='left', on='numprocesso')
    else:
        lic['qtd_itens'] = 0
    limits = LegalLimits.from_csv()
    lic['lsl_cap'] = lic['tipo'].apply(lambda t: limits.lsl_for(regime, t))
    lic['razao_vt_lsl'] = lic['valor_total'] / lic['lsl_cap']
    lic['flag_irregular'] = (lic['valor_total'] > lic['lsl_cap']).astype(int)
    lic['objeto_norm'] = lic['objeto'].astype(str).apply(normalize_text)
    lic['orgao_norm'] = lic['orgao'].astype(str).apply(normalize_text)
    keep = ['numprocesso','ano','modalidade','situacao','orgao','objeto','tipo','valor_estimado','valor_ou_desconto_homologado','valor_total','lsl_cap','razao_vt_lsl','flag_irregular','participantes','vencedores','qtd_itens']
    silver = lic[keep].copy()
    silver['excedente'] = (silver['valor_total'] - silver['lsl_cap']).clip(lower=0)
    return silver

def compute_kpis(silver: pd.DataFrame) -> pd.DataFrame:
    total_licit = len(silver)
    total_valor = silver['valor_total'].sum(skipna=True)
    irreg = silver['flag_irregular'].sum()
    exced = silver['excedente'].sum(skipna=True)
    idx_irregularidade = (irreg / total_licit * 100) if total_licit else 0.0
    idx_prejuizo = (exced / total_valor * 100) if total_valor else 0.0
    return pd.DataFrame({'total_licitacoes':[total_licit],'irregulares':[irreg],'indice_irregularidade_%':[round(idx_irregularidade,2)],'montante_total_brl':[round(total_valor,2)],'excedente_total_brl':[round(exced,2)],'indice_prejuizo_%':[round(idx_prejuizo,2)]})
