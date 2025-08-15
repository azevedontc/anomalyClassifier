from __future__ import annotations
import re
try:
    from unidecode import unidecode
except Exception:
    unidecode = None

STOP = {"de","da","do","para","em","kg","und","un","ml","lt","l","e","a","o","as","os","um","uma"}

def normalize_text(s: str) -> str:
    if not isinstance(s, str):
        s = "" if s is None else str(s)
    s = s.lower()
    s = unidecode(s) if unidecode else s.encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    tokens = [t for t in s.split() if t not in STOP and len(t) > 1]
    return " ".join(tokens)
