#!/usr/bin/env python3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from src.data_ingest import load_all
from src.preprocess import build_silver, compute_kpis
from src.config import DATA_SILVER, DEFAULT_REGIME

def main():
    data = load_all()
    silver = build_silver(data['licitacoes'], data['itens'], data['empresas'], regime=DEFAULT_REGIME)
    DATA_SILVER.mkdir(parents=True, exist_ok=True)
    silver_path = DATA_SILVER / 'silver.parquet'
    silver.to_parquet(silver_path, index=False)
    kpis = compute_kpis(silver)
    kpis.to_csv(DATA_SILVER / 'kpis.csv', index=False)
    print(f'Saved {silver.shape[0]} linhas em {silver_path}')
    print(kpis.to_string(index=False))

if __name__ == '__main__':
    main()
