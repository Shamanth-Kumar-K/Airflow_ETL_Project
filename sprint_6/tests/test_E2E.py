import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.pipeline import run_pipeline
from pathlib import Path

def test_end_to_end_pipeline(tmp_path):
    input_dir = Path("sprint_6/test_data/raw")
    output_dir = tmp_path / "output"

    run_pipeline(input_dir, output_dir)

    assert (output_dir / "customers_cleaned.csv").exists()
    assert (output_dir / "sales_enriched.csv").exists()
