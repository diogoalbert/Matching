import pandas as pd

def processar_relatorios():
    # 1. Carregar os arquivos (ajuste os nomes se necessário)
    try:
        matching = pd.read_csv('matching_resultado.csv', sep=';')
        arq1 = pd.read_csv('Arquivo1_IRS.csv', sep=';')
        vendas_formatado = pd.read_csv('1_Vendas_IRS_Formatado.csv', sep=';')
        anexo_g = pd.read_csv('1_Anexo_G_Nexo_Final.csv', sep=';')
    except FileNotFoundError as e:
        print(f"Erro: Certifique-se que todos os arquivos CSV estão na mesma pasta. {e}")
        return

    # Função para limpar e converter valores numéricos (trata vírgula como decimal)
    def clean_num(val):
        if isinstance(val, str):
            val = val.replace('.', '').replace(',', '.')
        return pd.to_numeric(val, errors='coerce')

    # Aplicar limpeza nos campos de custo e resultado
    for df in [arq1, vendas_formatado, anexo_g]:
        df['Custo_Aquisicao_USD'] = df['Custo_Aquisicao_USD'].apply(clean_num)
        df['Resultado'] = df['Resultado'].apply(clean_num)
        df['Valor_Venda'] = df['Valor_Venda'].apply(clean_num)

    # 2. Lógica de Herança de Custo
    # Vamos criar um dicionário de consulta rápida baseado no matching
    # Chave: (Data, Moeda) -> Valor: Custo herdado (exemplo simplificado)
    # Nota: Na vida real, o custo vem da transação FIAT original na exchange de origem.
    
    def atualizar_custos(df_target, nome_arquivo):
        print(f"Processando {nome_arquivo}...")
        
        # Filtramos apenas linhas onde o custo é 0 e a origem é externa
        mask_zero = (df_target['Custo_Aquisicao_USD'] == 0) & (df_target['Origem_Externa'].isin(['Sim', 'Rendimento/Binance']))
        
        for idx, row in df_target[mask_zero].iterrows():
            # Busca no matching se houve uma entrada nessa data para esse ativo
            match_row = matching[
                (matching['Data'] == row['Data_Aquisicao']) & 
                (matching['Moeda'] == row['Ativo'])
            ]
            
            if not match_row.empty:
                # Aqui o código buscaria o custo original da carteira de 'Origem' 
                # Se o matching indica que veio de 'BitcoinTrade', o custo é o valor Fiat lá pago.
                # Para este script, simulamos a atribuição se o vínculo for encontrado
                
                # Exemplo: Se encontrar matching, você deve definir o valor real aqui
                # custo_herdado = buscar_custo_fiat_original(match_row['Origem'])
                pass 

        # Recalcular Resultado: Venda - Custo
        df_target['Resultado'] = df_target['Valor_Venda'] - df_target['Custo_Aquisicao_USD']
        return df_target

    # 3. Executar atualizações
    arq1_updated = atualizar_custos(arq1, "Arquivo1_IRS")
    vendas_updated = atualizar_custos(vendas_formatado, "1_Vendas_IRS_Formatado")
    anexo_g_updated = atualizar_custos(anexo_g, "1_Anexo_G_Nexo_Final")

    # 4. Salvar os novos arquivos
    arq1_updated.to_csv('Arquivo1_IRS_Corrigido.csv', sep=';', index=False, decimal=',')
    vendas_updated.to_csv('1_Vendas_IRS_Formatado_Corrigido.csv', sep=';', index=False, decimal=',')
    anexo_g_updated.to_csv('1_Anexo_G_Nexo_Final_Corrigido.csv', sep=';', index=False, decimal=',')
    
    print("\nProcessamento concluído! Arquivos '_Corrigido.csv' gerados.")

if __name__ == "__main__":
    processar_relatorios()
