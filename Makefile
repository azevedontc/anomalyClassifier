.PHONY: setup data app gold

setup:
	python -m venv .venv && . .venv/bin/activate && pip install -U pip && pip install -r requirements.txt

data:
	python scripts/build_silver.py

gold:
	python scripts/build_gold.py

app:
	streamlit run app.py
