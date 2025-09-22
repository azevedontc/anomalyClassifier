import re, unicodedata
from typing import Optional
import pandas as pd

def slug_col(name: str) -> str:
    name = unicodedata.normalize("NFKD", name)
    name = "".join([c for c in name if not unicodedata.combining(c)])
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name.lower()

def read_csv_safely(path: str) -> Optional[pd.DataFrame]:
    for sep in [",",";","|","\t"]:
        for enc in ["utf-8", "latin1", "cp1252"]:
            try:
                df = pd.read_csv(path, sep=sep, encoding=enc, dtype=str, low_memory=False)
                if df.shape[1] == 1 and sep in str(df.iloc[0,0])[:200]:
                    continue
                df.columns = [slug_col(c) for c in df.columns]
                for c in df.columns:
                    if df[c].dtype == object:
                        df[c] = df[c].astype(str).str.strip()
                return df
            except Exception:
                continue
    return None
