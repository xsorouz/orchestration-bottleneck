
import pandas as pd

df = pd.read_csv("data/outputs/vins_millesimes.csv")
expected = 30
actual = df.shape[0]

assert actual == expected, f"❌ Nombre de vins millésimés incorrect : attendu {expected}, obtenu {actual}"
print("✅ Test Z-score : OK")
