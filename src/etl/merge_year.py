#!/usr/bin/env python3
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
from .merge_core import merge_one_folder

def main():
    ap = argparse.ArgumentParser(description="Merge de um ano (pasta do portal) em um arquivo único.")
    ap.add_argument("--input", required=True, help="pasta do ano (ex.: data/LICITACOES-2025)")
    ap.add_argument("--out", required=True, help="arquivo de saída (.parquet ou .csv)")
    args = ap.parse_args()

    folder = Path(args.input)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    df = merge_one_folder(folder)

    try:
        if out.suffix == ".parquet":
            df.to_parquet(out, index=False)
        else:
            df.to_parquet(out.with_suffix(".parquet"), index=False)
        print(f"[ok] salvo parquet em: {out if out.suffix == '.parquet' else out.with_suffix('.parquet')}")
    except Exception as e:
        print(f"[warn] parquet indisponível ({e}); salvando CSV…")
        if out.suffix == ".csv":
            df.to_csv(out if out.suffix == ".csv" else out.with_suffix(".csv"),
                      index=False, encoding="utf-8-sig")

    print("linhas:", len(df))
    print("colunas:", list(df.columns))

if __name__ == "__main__":
    main()
