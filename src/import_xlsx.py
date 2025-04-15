
import duckdb
import pandas as pd

# Lecture des fichiers Excel avec pandas
erp = pd.read_excel("data/raw/Fichier_erp.xlsx")
web = pd.read_excel("data/raw/Fichier_web.xlsx")
liaison = pd.read_excel("data/raw/fichier_liaison.xlsx")

# Connexion DuckDB en mémoire
con = duckdb.connect()

# Enregistrement des DataFrames comme tables
con.register("erp_raw", erp)
con.register("web_raw", web)
con.register("liaison_raw", liaison)

# Confirmation console
print("✅ Fichiers Excel importés et enregistrés en tables DuckDB.")
