# Azevedo TCC — Detecção de Anomalias em Licitações (Construção/Obras)

Pipeline **não supervisionado** para detectar **licitações anômalas** no Estado do Paraná, focado no nicho **Construção/Obras**.  
A saída final é um CSV/Parquet com `anomaly_score` e `is_anomalous` (sem texto de explicação).  
A **explicação** é calculada **no front-end (Streamlit)** de forma leve e intuitiva.

## Estrutura do Projeto
```
azevedo-tcc-licitacoes/
├─ app/
│  └─ app.py                  # Front-end (Streamlit) — lê CSV/Parquet final e mostra resultados/explicações
├─ data/                      # CSVs brutos (por ano), mesmo layout para 2025/2024/2023
├─ outputs/
│  ├─ 2025/
│  │  ├─ licitacoes_construcao_obras_merged-2025.csv
│  │  ├─ licitacoes_construcao_obras_anomalias-2025.csv
│  │  └─ modelmeta-2025.json  # metadados p/ explicações (medianas/IQR/features/contamination)
│  └─ 2024/
│     ├─ licitacoes_construcao_obras_merged-2024.csv
│     ├─ licitacoes_construcao_obras_anomalias-2024.csv
│     └─ modelmeta-2024.json
├─ src/
│  ├─ __init__.py
│  ├─ config.py               # Palavras-chave do nicho e parâmetros padrão
│  ├─ io_utils.py             # Leitura robusta e normalização de colunas
│  ├─ features.py             # Seleção/criação de colunas numéricas e agregações
│  └─ pipeline.py             # Função `run_year(...)` que orquestra tudo e escreve saídas
├─ requirements.txt
├─ Makefile
└─ README.md
```

## Como rodar
### 1) Instalar dependências
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2) Executar o pipeline (por ano)
```bash
# Processar 2025
python -m src.pipeline --input_dir ./data --year 2025 --out_dir ./outputs

# Processar 2024
python -m src.pipeline --input_dir ./data --year 2024 --out_dir ./outputs

# Quando chegar 2023 (mesma estrutura)
python -m src.pipeline --input_dir ./data --year 2023 --out_dir ./outputs
```

Saídas por ano:
- `*_merged-<ano>.csv` — dataset do nicho com features agregadas
- `*_anomalias-<ano>.csv` — resultado final da classificação (sem col. de explicação)
- `modelmeta-<ano>.json` — metadados para explicar no front-end (medianas, IQR, features, threshold/contamination)

### 3) Rodar o front-end (Streamlit)
```bash
streamlit run app/app.py
```
No app:
- Carregue os arquivos do ano (`anomalias-<ano>.csv` + opcionalmente `merged-<ano>.csv` e `modelmeta-<ano>.json`);
- Explore as anomalias, filtre, e clique em um registro para ver **explicação** dos **top desvios** (IQR) e gráficos.

## Observações
- **Não é fraude**: “anômalo” significa **fora do padrão** dos pares.
- O filtro do nicho usa palavras-chave no texto do objeto/descrição; se houver uma coluna categórica oficial de classe, substitua no `src/config.py`.
- O `contamination` pode ser ajustado no `config.py` ou via CLI em `src/pipeline.py`.

