#!/usr/bin/env python3
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
from .merge_core import merge_one_folder

def main():
    ap = argparse.ArgumentParser(description="Merge multi‑ano (concatena vários anos).")
    ap.add_argument("--folders", nargs="+", required=True,
                    help="pastas por ano (ex.: data/LICITACOES-2025 data/LICITACOES-2024 data/LICITACOES-2023)")
    ap.add_argument("--out", required=True, help="arquivo de saída (.parquet ou .csv)")
    args = ap.parse_args()

    frames = []
    for f in args.folders:
        print(f"[merge] {f}")
        frames.append(merge_one_folder(Path(f)))
    df = pd.concat(frames, ignore_index=True)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
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
