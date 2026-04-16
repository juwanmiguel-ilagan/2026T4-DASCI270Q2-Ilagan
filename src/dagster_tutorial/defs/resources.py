import json
from pathlib import Path

import dagster as dg
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]


class SalesIO(dg.ConfigurableResource):
    source_csv: str
    output_dir: str

    def read_sales_csv(self) -> pd.DataFrame:
        df = pd.read_csv(self.source_csv, encoding="utf-8-sig")
        df.columns = (
            df.columns.astype(str)
            .str.replace("\ufeff", "", regex=False)
            .str.strip()
        )
        return df

    def _ensure_output_dir(self) -> Path:
        path = Path(self.output_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_csv(self, name: str, df: pd.DataFrame) -> None:
        out = self._ensure_output_dir() / f"{name}.csv"
        df.to_csv(out, index=False)

    def write_json(self, name: str, payload: dict) -> None:
        out = self._ensure_output_dir() / f"{name}.json"
        out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "sales_io": SalesIO(
                source_csv=str(PROJECT_ROOT / "src" / "data" / "sales_2026_04_14.csv"),
                output_dir=str(PROJECT_ROOT / "src" / "data" / "outputs"),
            )
        }
    )