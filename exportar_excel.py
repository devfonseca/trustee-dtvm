import sqlite3
import pandas as pd
from pathlib import Path

# Caminhos dos arquivos (ajustados para a estrutura do seu projeto)
DB_PATH = Path("data/fiduciario.db")
EXCEL_PATH = Path("relatorio_fiduciario.xlsx")

def exportar_para_excel():
    if not DB_PATH.exists():
        print(f"[ERRO] Banco de dados não encontrado em: {DB_PATH}")
        return

    print("[1/3] Conectando ao banco de dados SQLite...")
    conn = sqlite3.connect(DB_PATH)

    try:
        # Puxando os dados das tabelas direto para DataFrames do Pandas
        print("[2/3] Extraindo tabelas 'ativos' e 'pu_historico'...")
        
        # Você pode customizar o SELECT aqui se não quiser todas as colunas
        df_ativos = pd.read_sql_query("SELECT * FROM ativos", conn)
        df_historico = pd.read_sql_query("SELECT * FROM pu_historico", conn)

        # Salvando em um arquivo Excel com múltiplas abas (sheets)
        print(f"[3/3] Gerando o arquivo Excel: {EXCEL_PATH} ...")
        
        with pd.ExcelWriter(EXCEL_PATH, engine='openpyxl') as writer:
            df_ativos.to_excel(writer, sheet_name='Ativos_Características', index=False)
            df_historico.to_excel(writer, sheet_name='Historico_Preços', index=False)
            
        print("\n✅ Sucesso! O arquivo Excel foi gerado e está pronto para uso.")

    except Exception as e:
        print(f"\n❌ Ocorreu um erro: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    exportar_para_excel()