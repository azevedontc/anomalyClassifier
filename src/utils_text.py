import re
import unicodedata
from typing import Optional

KEYWORDS_OBRA = ['obra','engenharia','reforma','pavimenta','constru','edifica','recape','manutencao predial','obras','pontes','drenagem']

def normalize_text(s: Optional[str]) -> str:
    if s is None:
        return ''
    s = str(s).lower().strip()
    s = unicodedata.normalize('NFKD', s).encode('ascii','ignore').decode('ascii')
    s = re.sub(r"[^a-z0-9\s]", ' ', s)
    s = re.sub(r"\s+", ' ', s).strip()
    return s

def classify_tipo(objeto: str) -> str:
    txt = normalize_text(objeto)
    for kw in KEYWORDS_OBRA:
        if kw in txt:
            return 'obra_eng'
    return 'bens_servicos'
