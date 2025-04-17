import duckdb

con = duckdb.connect("data.duckdb")

# Nombre total de lignes dans fusion
total = con.execute("SELECT COUNT(*) FROM fusion").fetchone()[0]

# Lignes avec stock_quantity > 0 AND stock_status = 'instock'
valid = con.execute("""
    SELECT COUNT(*) FROM fusion
    WHERE stock_quantity > 0
      AND stock_status = 'instock'
""").fetchone()[0]

# Somme brute du CA
ca_total = con.execute("""
    SELECT ROUND(SUM(price * stock_quantity), 2)
    FROM fusion
    WHERE stock_quantity > 0
      AND stock_status = 'instock'
""").fetchone()[0]

print(f"Fusion : {total} lignes")
print(f"Valides pour le CA : {valid} lignes")
print(f"CA brut calculé     : {ca_total} €")
