# anomalyTracker
Anomaly Detector in Public Bidding / Detector de Anomalias em Licitações Públicas 
## Como é calculado o score?
```
Para cada item de licitação, o sistema cria medidas objetivas:
- Preço por unidade (estimado e adjudicado).
- Preço típico do grupo (mediana de itens semelhantes).
- Quão acima do normal o preço está (estatísticas como z-score e IQR).
- Desconto (quanto caiu do estimado pro adjudicado).
- Concorrência (quantos CNPJs distintos deram lance no lote).
- Concentração (quantas vitórias o mesmo CNPJ tem dentro do mesmo órgão).

O sistema faz duas pontuações:
    - Regras (0–100): cada “bandeira vermelha” soma um peso.
    Pesos padrão:
        - preço muito acima: 40
        - baixa concorrência (≤1 proposta): 25
        - desconto muito baixo (<2%): 20
        - fornecedor concentrado na UG: 15
    Soma dos pesos acionados → normaliza para 0–100.
    Modelo automático (0–100): IsolationForest olha todas as variáveis juntas e diz “o quão fora da curva” o item está (0=normal, 100=mais estranho).

A nota final combina os 2 jeitos:

score = 0.6 × nota do modelo + 0.4 × nota das regras

Ex.: se o modelo deu 80 e as regras somaram 60 → score = 0.6 × 80 + 0.4 × 60 = 72.

Importante: score alto não acusa fraude. Ele só diz: “vale olhar com carinho”.
```
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
