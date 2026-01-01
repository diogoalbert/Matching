import pandas as pd

# 1. Carregar os arquivos
nexo = pd.read_csv('Nexo.csv', sep=';')
binance = pd.read_csv('Binance.csv', sep=';')
btctrade = pd.read_csv('BitcoinTrade.csv', sep=';')

def preparar_dados(df, nome_exchange):
    # Remover espaços em branco e padronizar nomes de colunas
    df.columns = df.columns.str.strip()
    
    # Padronizar nomes de moedas comuns na BitcoinTrade
    mapa_moedas = {'Bitcoin': 'BTC', 'Ethereum': 'ETH', 'Tether': 'USDT', 'Litecoin': 'LTC'}
    if 'Moeda' in df.columns:
        df['Moeda'] = df['Moeda'].replace(mapa_moedas)
    
    # Converter quantidades para float (ajustando vírgula para ponto)
    col_qtd = 'Quantidade' if 'Quantidade' in df.columns else 'Qtd'
    df[col_qtd] = df[col_qtd].astype(str).str.replace(',', '.').astype(float)
    
    # Identificar Direção (Entrada ou Saída)
    def definir_direcao(tipo):
        tipo = str(tipo).upper()
        if any(x in tipo for x in ['ENTRADA', 'DEPÓSITO', 'COMPRA', 'ORIGEM']):
            return 'IN'
        if any(x in tipo for x in ['SAIDA', 'RETIRADA', 'VENDA', 'PARA CARTEIRA']):
            return 'OUT'
        return 'OTHER'

    df['Direcao'] = df['Tipo'].apply(definir_direcao)
    df['Exchange'] = nome_exchange
    
    # Remover duplicatas exatas (muito comum no arquivo BitcoinTrade)
    return df.drop_duplicates()

# Preparar os 3 DataFrames
nexo_clean = preparar_dados(nexo, 'Nexo')
binance_clean = preparar_dados(binance, 'Binance')
btctrade_clean = preparar_dados(btctrade, 'BitcoinTrade')

# Renomear colunas para unificar
nexo_clean = nexo_clean.rename(columns={'Quantidade': 'Valor'})
binance_clean = binance_clean.rename(columns={'Qtd': 'Valor'})
btctrade_clean = btctrade_clean.rename(columns={'Qtd': 'Valor'})

# Criar listas globais de Saídas e Entradas
todas_saidas = pd.concat([
    nexo_clean[nexo_clean['Direcao'] == 'OUT'],
    binance_clean[binance_clean['Direcao'] == 'OUT'],
    btctrade_clean[btctrade_clean['Direcao'] == 'OUT']
])

todas_entradas = pd.concat([
    nexo_clean[nexo_clean['Direcao'] == 'IN'],
    binance_clean[binance_clean['Direcao'] == 'IN'],
    btctrade_clean[btctrade_clean['Direcao'] == 'IN']
])

# Lógica de Matching
resultados = []
indices_entrada_usados = set()

for i, saida in todas_saidas.iterrows():
    # Busca entrada com: Mesma Moeda, Mesmo Valor, Mesma Data, Exchange Diferente
    match = todas_entradas[
        (todas_entradas['Moeda'] == saida['Moeda']) &
        (abs(todas_entradas['Valor'] - saida['Valor']) < 1e-8) & # Margem para erro de float
        (todas_entradas['Data'] == saida['Data']) &
        (todas_entradas['Exchange'] != saida['Exchange']) &
        (~todas_entradas.index.isin(indices_entrada_usados))
    ]
    
    if not match.empty:
        idx_match = match.index[0]
        entrada = match.iloc[0]
        resultados.append({
            'Data': saida['Data'],
            'Moeda': saida['Moeda'],
            'Quantidade': saida['Valor'],
            'Origem': saida['Exchange'],
            'Destino': entrada['Exchange'],
            'Hora Saída': saida['Hora'],
            'Hora Entrada': entrada['Hora']
        })
        indices_entrada_usados.add(idx_match)

# Salvar Resultado
df_final = pd.DataFrame(resultados)
df_final.to_csv('matching_resultado.csv', index=False, sep=';', encoding='utf-8-sig')

print(f"Processamento concluído. {len(df_final)} matches encontrados.")
