import sidrapy
import pandas as pd

def ingest_ibge_data():
    # Tabela 5938: PIB a preços correntes e impostos
    # Variável 37: Produto Interno Bruto a preços correntes
    # Território: n6 (todos os municípios do Brasil)
    # Período: last (último ano disponível)
    data = sidrapy.get_table(
        table_code="5938",
        territorial_level="6",
        ibge_territorial_code="all",
        variable="37",
        period="last"
    )

    df_pib = pd.DataFrame(data)
    df_pib.columns = df_pib.iloc[0]
    df_pib = df_pib.iloc[1:]
    df_pib = df_pib.rename(columns={
        'Valor': 'VALOR',
        'Município (Código)': 'COD_MUNICIPIO',
        'Município': 'NOM_MUNICIPIO'
    })
    df_pib = df_pib[['COD_MUNICIPIO', 'NOM_MUNICIPIO', 'VALOR']]
    df_pib.to_csv('/tmp/IBGE_PIB_MUNICIPIOS.csv', index=False, sep=';')


if __name__ == "__main__":
    ingest_ibge_data()