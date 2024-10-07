import os
import pandas as pd

# Função para converter CSV para Parquet
def csv_to_parquet(csv_path, parquet_path, sep=';', engine='pyarrow'):
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, sep=sep, low_memory=False)
        df.to_parquet(parquet_path, engine=engine)
        print(f"Convertido: {csv_path} -> {parquet_path}")
    else:
        print(f"Arquivo não encontrado: {csv_path}")

# Dicionário com os caminhos dos arquivos
file_paths = {
    'dim_convenio': r'\\hpbisql01\importacoes\Analytics\08.Arquivos Gerados\Matriz_Produtos\Dim_Convenio.csv'
}

# Converter cada arquivo CSV para Parquet
for key, csv_path in file_paths.items():
    parquet_path = csv_path.replace('.csv', '.parquet')
    csv_to_parquet(csv_path, parquet_path)
