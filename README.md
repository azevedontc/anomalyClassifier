# anomalyTracker

Detector simples de anomalias em licitações do **Paraná (2023–2025)**.

## Como executar localmente
```bash
pip install -r requirements.txt
python3 scripts/build_silver.py    # gera data/silver/*.parquet e KPIs
python3 scripts/build_gold.py      # gera curadorias em data/gold/
streamlit run app.py               # front-end
```

## Metodologia (resumo)
- **Valor total (VT)**: `valor_ou_desconto_homologado` quando disponivel; caso contrario, `valor_estimado`.
- **Classificação do objeto**: heuristica de palavras-chave para `obra_eng` vs `bens_servicos` (`src/utils_text.py`).
- **LSL (Limite Superior de Referência)**: tabela `data/limits_br.csv` por **regime** (8666/14133) e **tipo**.
- **Indicador**: **Razão VT/LSL**. Sinalizamos `flag_irregular = 1` quando `VT > LSL`.
- **KPIs**: % irregulares e % de “prejuízo” (excedente/total) no recorte.

### Sobre o LSL e os fundamentos legais
**Proposito didatico**: o LSL e um *cap* de referência para comparar valores. A planilha `data/limits_br.csv` contém valores **exemplificativos**.

- **Lei 8.666/1993 (revogada)** — **art. 23**: definia faixas de valor para modalidades. Valores historicamente usados como referência eram, por exemplo, **R$ 1.500.000** (obras/servicos de engenharia) e **R$ 650.000** (compras/serviços) para concorrência.
- **Lei 14.133/2021** — mantém modalidades diferentes e **define limites para dispensa por valor** (art. 75). Não há mais a mesma logica de “faixas por modalidade” da 8.666; portanto, qualquer *cap* precisa ser **explicitado como critério do estudo**.

> **Aviso**: Ajuste a `limits_br.csv` conforme a fundamentação escolhida (legislação aplicável ao período, decretos, atualizações).

## Dicionário de dados (silver)
| coluna | descricao |
|---|---|
| numprocesso | Chave do processo |
| ano | Ano da licitacao |
| modalidade | Modalidade declarada |
| situacao | Status do processo |
| orgao | Orgao demandante |
| objeto | Descricao do objeto |
| tipo | obra_eng / bens_servicos (heuristica) |
| valor_estimado | Valor estimado |
| valor_ou_desconto_homologado | Valor homologado |
| valor_total | VT = homologado ou estimado |
| lsl_cap | Limite superior (tabela + regime) |
| razao_vt_lsl | VT/LSL |
| flag_irregular | 1 se VT > LSL |
| participantes | Empresas unicas participantes |
| vencedores | Vencedores no processo |
| qtd_itens | Itens do processo |
| excedente | max(VT-LSL, 0) |

## Saídas gold
- `data/gold/ranking_processos.csv` — ranking por VT/LSL (com excedente).
- `data/gold/kpis_por_orgao_ano.csv` — KPIs por orgão/ano.

## Observações
- Unidades e valores dependem da qualidade dos CSVs e da parametrização do LSL.
- `top winners/losers` agora mede **participação por lote**; taxa de vitoria fica <= 100%.
