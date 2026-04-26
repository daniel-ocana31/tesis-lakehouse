import duckdb

ruta = "data/bronze/sbs/anio=2025/mes=12/B-3243_Ranking_Creditos_Depositos_Patrimonio.parquet"
df = duckdb.query(f"SELECT * FROM '{ruta}'").df()
print(df.shape)
print(df.to_string())