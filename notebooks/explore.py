import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

df = pd.read_parquet("data/raw/yellow_tripdata_2023-01.parquet")

print("SHAPE:", df.shape)
print("\n--- COLUMNS & TYPES ---")
print(df.dtypes)
print("\n--- FIRST 5 ROWS ---")
print(df.head())
print("\n--- NULL COUNTS ---")
print(df.isna().sum())
print("\n--- NUMERIC SUMMARY ---")
print(df[["passenger_count", "trip_distance", "fare_amount", "total_amount"]].describe())
print("\n--- CATEGORICAL VALUES ---")
for col in ["VendorID", "RatecodeID", "payment_type"]:
    print(f"\n{col}:")
    print(df[col].value_counts(dropna=False).head(10))
print("\n--- DATE RANGE ---")
print(df["tpep_pickup_datetime"].min(), "→", df["tpep_pickup_datetime"].max())