.PHONY: venv install run-2025 run-2024 app

venv:
	python -m venv .venv

install: venv
	. .venv/bin/activate && pip install -r requirements.txt

run-2025:
	python -m src.pipeline --input_dir ./data --year 2025 --out_dir ./outputs

run-2024:
	python -m src.pipeline --input_dir ./data --year 2024 --out_dir ./outputs

app:
	streamlit run app/app.py
