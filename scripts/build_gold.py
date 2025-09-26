#!/usr/bin/env python3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from src.data_ingest import load_all
from src.preprocess import build_silver, compute_kpis
from src.config import DATA_GOLD, DEFAULT_REGIME

def main():
    data = load_all()
    silver = build_silver(data['licitacoes'], data['itens'], data['empresas'], regime=DEFAULT_REGIME)
    DATA_GOLD.mkdir(parents=True, exist_ok=True)
    # ranking completo por processo
    rank_cols = ['numprocesso','ano','orgao','modalidade','valor_total','lsl_cap','razao_vt_lsl','flag_irregular','participantes','vencedores','qtd_itens','excedente']
    silver[rank_cols].sort_values(['razao_vt_lsl','valor_total'], ascending=False).to_csv(DATA_GOLD/'ranking_processos.csv', index=False)
    # KPIs por orgao e ano
    kpi = silver.groupby(['orgao','ano']).agg(
        total=('numprocesso','nunique'),
        irreg=('flag_irregular','sum'),
        montante=('valor_total','sum'),
        excedente=('excedente','sum')
    ).reset_index()
    kpi['indice_irregularidade_%'] = (kpi['irreg']/kpi['total']*100).round(2)
    kpi['indice_prejuizo_%'] = (kpi['excedente']/kpi['montante']*100).round(2)
    kpi.to_csv(DATA_GOLD/'kpis_por_orgao_ano.csv', index=False)
    print('Arquivos gerados em data/gold: ranking_processos.csv, kpis_por_orgao_ano.csv')

if __name__ == '__main__':
    main()
