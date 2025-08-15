# anomalyTracker
Anomaly Detector in Public Bidding / Detector de Anomalias em Licitações Públicas 

## 1) Criar venv e instalar deps
```
python3 -m venv .venv && source .venv/bin/activate
python3 -m pip install --upgrade pip
pip install -r requirements.txt
```

## 2) Organizar os CSVs do portal
```
data/
  LICITACOES-2025/  -> TB_ITENS-2025.csv, TB_LICITACOES-2025.csv,
                       TB_LOTES-2025.csv, TB_EMPRESAS_PARTICIPANTES-2025.csv,
                       (opcional) TB_PUBLICIDADE-2025.csv
  LICITACOES-2024/  -> mesmos nomes com -2024
  LICITACOES-2023/  -> mesmos nomes com -2023
```

## 3A) Merge de um ano
```
python3 -m src.etl.merge_year --input data/LICITACOES-2025 --out data/licitacoes_2025.parquet
```

(se der erro de parquet, o script salva CSV automaticamente)


## 3B) Merge multi‑ano (recomendado)
```
python3 -m src.etl.merge_multi_years \
  --folders data/LICITACOES-2025 data/LICITACOES-2024 data/LICITACOES-2023 \
  --out data/licitacoes_2023_2025.parquet
```

## 4) Gerar o ranking de anomalias
```
python3 -m src.pipeline.score_from_merged \
  --input data/licitacoes_2023_2025.parquet \
  --out data/anomalias_2023_2025.csv
```

## 5) Abrir o dashboard
```
python3 -m streamlit run src/app/dashboard.py
```
