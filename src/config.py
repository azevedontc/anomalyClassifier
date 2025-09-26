from pathlib import Path
import os

ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / 'data' / 'raw'
DATA_BRONZE = ROOT / 'data' / 'bronze'
DATA_SILVER = ROOT / 'data' / 'silver'
DATA_GOLD = ROOT / 'data' / 'gold'

_external_env = os.environ.get('EXTERNAL_DATA_DIR')
EXTERNAL_DATA_DIR = Path(_external_env) if _external_env else DATA_BRONZE
YEARS = [2023, 2024, 2025]
UF = 'PR'
DEFAULT_REGIME = os.environ.get('LEGAL_REGIME', '8666')
