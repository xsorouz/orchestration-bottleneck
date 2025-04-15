
import duckdb

df = duckdb.sql("SELECT * FROM ca_total").to_df()
expected = 70568.60
actual = round(df['ca_total'][0], 2)

assert actual == expected, f"❌ CA total incorrect : attendu {expected}, obtenu {actual}"
print("✅ Test CA total : OK")
