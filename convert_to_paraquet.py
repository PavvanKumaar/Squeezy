
from pathlib import Path
import pandas as pd

# Existing dataset
source = Path(r"C:\Users\harsh\Squeezy\data\training\v1_1_raw.pkl")

# Output file: same directory
destination = source.with_suffix(".parquet")

def main():
    if not source.exists():
        raise FileNotFoundError(f"Pickle file not found: {source}")

    print(f"Loading: {source}")
    df = pd.read_pickle(source)

    print(f"Dataset shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")

    print(f"Converting to Parquet: {destination}")
    df.to_parquet(
        destination,
        engine="pyarrow",
        index=False,
        compression="snappy"
    )

    print("Conversion successful!")
    print(f"Output: {destination}")
    print(f"File size: {destination.stat().st_size:,} bytes")

    # Verify that the saved file can be read
    check = pd.read_parquet(destination, engine="pyarrow")
    assert check.shape == df.shape, "Verification failed: shape mismatch"

    print("Verification passed: row and column counts match.")

if __name__ == "__main__":
    main()
