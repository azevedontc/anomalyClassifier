from pathlib import Path
import pandas as pd
import numpy as np
from typing import List, Dict
from .config import EXTERNAL_DATA_DIR, YEARS, DATA_BRONZE

def _read_csv_any(path: Path, nrows=None) -> pd.DataFrame:
    try:
        return pd.read_csv(path, sep=';', dtype=str, nrows=nrows, encoding='utf-8')
    except Exception:
        try:
            return pd.read_csv(path, sep=',', dtype=str, nrows=nrows, encoding='utf-8')
        except Exception:
            return pd.read_csv(path, sep=';', dtype=str, nrows=nrows, encoding='latin1')

def load_concat(pattern: str, years: List[int] = YEARS, prefer_bronze=True) -> pd.DataFrame:
    frames = []
    base_dirs = [DATA_BRONZE, EXTERNAL_DATA_DIR]
    for y in years:
        fname = f"{pattern}-{y}.csv"
        # procura primeiro em data/bronze e depois no EXTERNAL_DATA_DIR
        found = None
        for base in base_dirs:
            fpath = base / fname
            if fpath.exists():
                found = fpath; break
        if found is None:
            continue
        df = _read_csv_any(found)
        df['ano'] = df.get('ano', str(y))
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True).drop_duplicates()
    return out

def load_all(years: List[int] = YEARS) -> Dict[str, pd.DataFrame]:
    lic = load_concat('TB_LICITACOES', years)
    its = load_concat('TB_ITENS', years)
    emp = load_concat('TB_EMPRESAS_PARTICIPANTES', years)
    org = load_concat('TB_ORGAOS_PARTICIPANTES', years)
    ades = load_concat('TB_ADESAO_ATA_SRP', years)
    return {'licitacoes': lic, 'itens': its, 'empresas': emp, 'orgaos': org, 'adesoes': ades}
