from dataclasses import dataclass
from pathlib import Path
import pandas as pd

from .config import ROOT

@dataclass
class LegalLimits:
    table: pd.DataFrame

    @classmethod
    def from_csv(cls, path: Path = ROOT / 'data' / 'limits_br.csv'):
        df = pd.read_csv(path, dtype={'regime': str, 'tipo': str})
        df['regime'] = df['regime'].astype(str)
        df['tipo'] = df['tipo'].astype(str)
        return cls(table=df)

    def lsl_for(self, regime: str, tipo: str) -> float:
        row = self.table[(self.table['regime']==str(regime)) & (self.table['tipo']==str(tipo))]
        if row.empty:
            return float('nan')
        return float(row.iloc[0]['lsl_cap_brl'])
