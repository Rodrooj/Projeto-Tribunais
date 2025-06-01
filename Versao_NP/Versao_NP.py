# -*- coding: utf-8 -*-
"""
Versão Não Paralela (NP) para processamento de dados de tribunais.

Este script realiza o processo ETL (Extract, Transform, Load) nos arquivos CSV
disponibilizados, calcula as metas de desempenho dos tribunais conforme
especificado no documento TP06 e gera arquivos consolidados e de resumo.
"""

import pandas as pd
import os
import glob
import time
import numpy as np
import warnings
import matplotlib.pyplot as plt

# Ignorar warnings específicos do Pandas que podem ocorrer durante conversões ou divisões por zero
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

# --- Configurações Iniciais ---
DIRETORIO_ENTRADA = "/Users/rodrigo/Documents/Projetos VS Code/Projeto-Tribunais/Dados" # Diretório onde os CSVs de entrada estão
ARQUIVO_CONSOLIDADO = "/Users/rodrigo/Documents/Projetos VS Code/Projeto-Tribunais/Versao_NP/Consolidado.csv"
ARQUIVO_RESUMO_METAS = "/Users/rodrigo/Documents/Projetos VS Code/Projeto-Tribunais/Versao_NP/ResumoMetas.csv"
ARQUIVO_GRAFICO = "/Users/rodrigo/Documents/Projetos VS Code/Projeto-Tribunais/Versao_NP/grafico_meta1.png"
ARQUIVO_RELATORIO_SPEEDUP = "/Users/rodrigo/Documents/Projetos VS Code/Projeto-Tribunais/Versao_NP/speedup_report.pdf"
ARQUIVO_TEMPO_NP = "/Users/rodrigo/Documents/Projetos VS Code/Projeto-Tribunais/Versao_NP/tempo_np.txt" # Para guardar tempo para speedup

# Colunas que precisam ser numéricas para os cálculos das metas
# Baseado nas fórmulas do TP06 e na análise do CSV de exemplo
# Adicionando colunas que podem existir em outros arquivos/ramos
COLUNAS_NUMERICAS = [
    'casos_novos_2025', 'julgados_2025', 'suspensos_2025', 'dessobrestados_2025',
    # Meta 1 (já inclusas acima)
    # Meta 2
    'distm2_a', 'julgm2_a', 'suspm2_a',
    'distm2_b', 'julgm2_b', 'suspm2_b',
    'distm2_c', 'julgm2_c', 'suspm2_c',
    'distm2_ant', 'julgm2_ant', 'suspm2_ant',
    # Meta 4
    'distm4_a', 'julgm4_a', 'suspm4_a',
    'distm4_b', 'julgm4_b', 'suspm4_b',
    # Meta 6
    'distm6', 'julgm6', 'suspm6',
    'distm6_a', 'julgm6_a', 'suspm6_a', # STJ usa _a no exemplo
    # Meta 7
    'distm7_a', 'julgm7_a', 'suspm7_a',
    'distm7_b', 'julgm7_b', 'suspm7_b',
    # Meta 8
    'distm8', 'julgm8', 'suspm8', # STJ
    'distm8_a', 'julgm8_a', 'suspm8_a',
    'distm8_b', 'julgm8_b', 'suspm8_b',
    # Meta 10
    'distm10', 'julgm10', 'suspm10', # STJ
    'distm10_a', 'julgm10_a', 'suspm10_a',
    'distm10_b', 'julgm10_b', 'suspm10_b'
]

# --- Funções Auxiliares ---
def safe_division(numerador, denominador, fator=100):
    """Realiza divisão segura, retornando 0 se o denominador for 0 ou NaN."""
    if pd.isna(denominador) or denominador == 0:
        return 0
    if pd.isna(numerador):
        return 0 # Ou talvez np.nan dependendo da regra de negócio para NA
    return (numerador / denominador) * fator

def get_sum(df, coluna):
    """Retorna a soma de uma coluna, tratando caso a coluna não exista."""
    return df[coluna].sum() if coluna in df.columns else 0

# --- Etapa 1: Extração e Concatenação (NP) ---
def extrair_e_concatenar_csv(diretorio):
    """Localiza, lê e concatena todos os arquivos CSV de um diretório."""
    print(f"Procurando arquivos CSV em: {diretorio}")
    arquivos_csv = glob.glob(os.path.join(diretorio, "*.csv"))
    if not arquivos_csv:
        print("Nenhum arquivo CSV encontrado no diretório.")
        return pd.DataFrame()
    print(f"Arquivos encontrados: {len(arquivos_csv)}")
    lista_dfs = []
    for arquivo in arquivos_csv:
        print(f"Lendo arquivo: {arquivo}")
        try:
            # CORREÇÃO: Usar vírgula como separador, conforme identificado no cabeçalho
            df_temp = pd.read_csv(
                arquivo,
                sep=",", # Alterado de ";" para ","
                encoding="latin1",
                dtype=str,
                low_memory=False
            )
            lista_dfs.append(df_temp)
            print(f" -> Lido com sucesso ({len(df_temp)} linhas).")
        except Exception as e:
            print(f"Erro ao ler o arquivo {arquivo}: {e}")
    if not lista_dfs:
        print("Nenhum DataFrame foi lido com sucesso.")
        return pd.DataFrame()
    print("Concatenando DataFrames...")
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    print(f"DataFrame consolidado criado com {len(df_consolidado)} linhas.")
    # Adicionar verificação das colunas após leitura
    print(f"Colunas após concatenação: {df_consolidado.columns.tolist()}")
    return df_consolidado

# --- Etapa 2: Limpeza e Pré-processamento (NP) ---
def limpar_e_preparar_dados(df):
    """Converte colunas para numérico e trata valores ausentes."""
    print("Iniciando limpeza e pré-processamento...")
    df_processado = df.copy()
    colunas_presentes = [col for col in COLUNAS_NUMERICAS if col in df_processado.columns]
    print(f"Colunas a serem convertidas para numérico: {len(colunas_presentes)}")

    # Verificar se 'sigla_tribunal' existe ANTES da conversão
    if 'sigla_tribunal' not in df_processado.columns:
        print("ALERTA: Coluna 'sigla_tribunal' não encontrada ANTES da conversão numérica.")
        # Poderia tentar encontrar uma coluna similar ou parar a execução

    for coluna in colunas_presentes:
        # A conversão só deve ocorrer se a coluna for do tipo 'object' (string)
        if df_processado[coluna].dtype == 'object':
            df_processado[coluna] = df_processado[coluna].str.strip().str.replace(',', '.', regex=False)
        # A conversão para numérico deve ser feita de qualquer forma para garantir o tipo correto
        df_processado[coluna] = pd.to_numeric(df_processado[coluna], errors='coerce')

    print("Preenchendo valores NaN nas colunas numéricas com 0 para cálculo.")
    df_processado[colunas_presentes] = df_processado[colunas_presentes].fillna(0)

    # Verificar se 'sigla_tribunal' existe DEPOIS da conversão
    if 'sigla_tribunal' not in df_processado.columns:
        print("ALERTA: Coluna 'sigla_tribunal' não encontrada DEPOIS da conversão numérica.")
    else:
        print("Coluna 'sigla_tribunal' presente após limpeza.")

    print("Limpeza e pré-processamento concluídos.")
    return df_processado

# --- Etapa 3: Cálculo das Metas (NP) ---

# Mapeamento de colunas (simplificado, assumindo que nomes no CSV são usados diretamente)
# As funções usarão get_sum para buscar os valores

def calcular_meta1(grupo):
    """Calcula a Meta 1 para um grupo (tribunal)."""
    julgados = get_sum(grupo, 'julgados_2025')
    casos_novos = get_sum(grupo, 'casos_novos_2025')
    dessobrestados = get_sum(grupo, 'dessobrestados_2025')
    suspensos = get_sum(grupo, 'suspensos_2025')
    denominador = casos_novos + dessobrestados - suspensos
    return safe_division(julgados, denominador, fator=100)

def calcular_metas_tribunal(grupo):
    """Calcula todas as metas aplicáveis para um grupo (tribunal)."""
    resultados = {}
    # Garantir que o grupo não está vazio antes de acessar iloc[0]
    if grupo.empty:
        return {"sigla_tribunal": "VAZIO", "ramo_justica": "VAZIO"} # Retorna um dict vazio ou com NAs

    # Usar .iloc[0] é seguro aqui porque estamos dentro de um grupo não vazio
    sigla = grupo['sigla_tribunal'].iloc[0]
    ramo = grupo['ramo_justica'].iloc[0]
    resultados['sigla_tribunal'] = sigla
    resultados['ramo_justica'] = ramo

    # --- Meta 1 (Comum a todos) ---
    resultados['Meta1'] = calcular_meta1(grupo)

    # --- Outras Metas (Exemplo para STJ, baseado no CSV e TP06) ---
    # É necessário implementar a lógica para CADA ramo e CADA meta
    if ramo == 'Superior Tribunal de Justiça':
        # Meta 2ANT (STJ)
        julgados = get_sum(grupo, 'julgm2_ant') # Assumindo coluna do CSV
        distribuidos = get_sum(grupo, 'distm2_ant')
        suspensos = get_sum(grupo, 'suspm2_ant')
        denominador = distribuidos - suspensos
        resultados['Meta2ANT'] = safe_division(julgados, denominador, fator=100)

        # Meta 4A (STJ)
        julgados = get_sum(grupo, 'julgm4_a')
        distribuidos = get_sum(grupo, 'distm4_a')
        suspensos = get_sum(grupo, 'suspm4_a')
        denominador = distribuidos - suspensos
        resultados['Meta4A'] = safe_division(julgados, denominador, fator=(1000/9))

        # Meta 4B (STJ)
        julgados = get_sum(grupo, 'julgm4_b')
        distribuidos = get_sum(grupo, 'distm4_b')
        suspensos = get_sum(grupo, 'suspm4_b')
        denominador = distribuidos - suspensos
        resultados['Meta4B'] = safe_division(julgados, denominador, fator=100)

        # Meta 6 (STJ) - Usando colunas com _a conforme exemplo CSV
        julgados = get_sum(grupo, 'julgm6_a')
        distribuidos = get_sum(grupo, 'distm6_a')
        suspensos = get_sum(grupo, 'suspm6_a')
        denominador = distribuidos - suspensos
        resultados['Meta6'] = safe_division(julgados, denominador, fator=(1000/7.5))

        # Meta 7A (STJ)
        julgados = get_sum(grupo, 'julgm7_a')
        distribuidos = get_sum(grupo, 'distm7_a')
        suspensos = get_sum(grupo, 'suspm7_a')
        denominador = distribuidos - suspensos
        resultados['Meta7A'] = safe_division(julgados, denominador, fator=(1000/7.5))

        # Meta 7B (STJ)
        julgados = get_sum(grupo, 'julgm7_b')
        distribuidos = get_sum(grupo, 'distm7_b')
        suspensos = get_sum(grupo, 'suspm7_b')
        denominador = distribuidos - suspensos
        resultados['Meta7B'] = safe_division(julgados, denominador, fator=(1000/7.5))

        # Meta 8 (STJ)
        julgados = get_sum(grupo, 'julgm8') # Usando coluna sem letra conforme TP06
        distribuidos = get_sum(grupo, 'distm8')
        suspensos = get_sum(grupo, 'suspm8')
        denominador = distribuidos - suspensos
        resultados['Meta8'] = safe_division(julgados, denominador, fator=(1000/10))

        # Meta 10 (STJ)
        julgados = get_sum(grupo, 'julgm10') # Usando coluna sem letra conforme TP06
        distribuidos = get_sum(grupo, 'distm10')
        suspensos = get_sum(grupo, 'suspm10')
        denominador = distribuidos - suspensos
        resultados['Meta10'] = safe_division(julgados, denominador, fator=(1000/10))

    # TODO: Implementar lógica para os outros ramos (Estadual, Trabalho, Federal, Militar, Eleitoral, TST)
    #       Cada 'if ramo == ...' deve conter os cálculos específicos daquele ramo.
    #       É crucial verificar os nomes exatos das colunas nos CSVs de cada ramo.

    return resultados

def calcular_todas_metas(df):
    """Agrupa por tribunal e calcula as metas para cada um."""
    print("Calculando metas por tribunal...")
    if 'sigla_tribunal' not in df.columns:
        print("Erro: Coluna 'sigla_tribunal' não encontrada para agrupamento.")
        # Imprimir colunas disponíveis para depuração
        print(f"Colunas disponíveis: {df.columns.tolist()}")
        return pd.DataFrame()

    grupos = df.groupby('sigla_tribunal')
    resultados_lista = []
    total_grupos = len(grupos)
    count = 0
    for nome_grupo, grupo_df in grupos:
        count += 1
        # Adicionar verificação se o grupo está vazio
        if grupo_df.empty:
            print(f"Aviso: Grupo {nome_grupo} está vazio, pulando cálculo.")
            continue
        print(f"Processando tribunal {count}/{total_grupos}: {nome_grupo}")
        resultado_tribunal = calcular_metas_tribunal(grupo_df)
        resultados_lista.append(resultado_tribunal)

    if not resultados_lista:
        print("Nenhum resultado de meta calculado.")
        return pd.DataFrame()

    df_resultados = pd.DataFrame(resultados_lista)
    print("Cálculo de metas concluído.")
    return df_resultados

# --- Etapa 4: Geração de Saídas (NP) ---
def gerar_saidas(df_resultados):
    """Gera o arquivo ResumoMetas.csv e o gráfico comparativo."""
    print("Gerando arquivos de saída...")

    # 4.1: Salvar ResumoMetas.csv
    try:
        # Arredondar para melhor visualização e preencher NaN com 'NA'
        # Garantir que colunas chave existem antes de preencher NA
        if 'sigla_tribunal' in df_resultados.columns:
             df_resumo = df_resultados.round(2).fillna('NA')
             # Certificar que colunas chave não fiquem como NA se forem resultado de grupo vazio
             df_resumo['sigla_tribunal'] = df_resumo['sigla_tribunal'].replace('NA', 'Desconhecido')
             df_resumo['ramo_justica'] = df_resumo['ramo_justica'].replace('NA', 'Desconhecido')
        else:
             print("Erro: Coluna 'sigla_tribunal' ausente no DataFrame de resultados.")
             df_resumo = pd.DataFrame() # Evitar erro no to_csv

        if not df_resumo.empty:
            print(f"Salvando resumo das metas em: {ARQUIVO_RESUMO_METAS}")
            df_resumo.to_csv(ARQUIVO_RESUMO_METAS, sep=";", index=False, encoding="utf-8")
            print(" -> Resumo salvo com sucesso.")
        else:
            print("Não foi possível gerar o resumo das metas.")

    except Exception as e:
        print(f"Erro ao salvar o arquivo ResumoMetas.csv: {e}")

    # 4.2: Gerar Gráfico Comparativo (Exemplo: Meta 1 por Ramo de Justiça)
    try:
        # Verificar se df_resultados não está vazio e contém as colunas necessárias
        if not df_resultados.empty and 'Meta1' in df_resultados.columns and 'ramo_justica' in df_resultados.columns:
            # Remover NAs antes de calcular a média
            meta1_validos = df_resultados.dropna(subset=['Meta1'])
            # Filtrar onde Meta1 não é 0 (assumindo que 0 significa não calculado ou divisão por zero)
            meta1_validos = meta1_validos[meta1_validos['Meta1'] != 0]

            if not meta1_validos.empty:
                media_meta1_ramo = meta1_validos.groupby('ramo_justica')['Meta1'].mean().sort_values()

                plt.figure(figsize=(12, 7))
                bars = plt.barh(media_meta1_ramo.index, media_meta1_ramo.values, color='skyblue')
                plt.xlabel('Cumprimento Médio da Meta 1 (%)')
                plt.ylabel('Ramo da Justiça')
                plt.title('Desempenho Médio da Meta 1 por Ramo da Justiça')
                plt.grid(axis='x', linestyle='--', alpha=0.7)
                plt.tight_layout()

                # Adicionar valores nas barras
                for bar in bars:
                    plt.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2, f'{bar.get_width():.1f}%', 
                             va='center', ha='left', fontsize=9)

                print(f"Salvando gráfico comparativo em: {ARQUIVO_GRAFICO}")
                plt.savefig(ARQUIVO_GRAFICO)
                plt.close() # Fechar a figura para liberar memória
                print(" -> Gráfico salvo com sucesso.")
            else:
                print("Não há dados válidos de Meta 1 para gerar o gráfico.")
        elif df_resultados.empty:
             print("DataFrame de resultados vazio, não é possível gerar gráfico.")
        else:
            print("Colunas 'Meta1' ou 'ramo_justica' não encontradas para gerar o gráfico.")
    except Exception as e:
        print(f"Erro ao gerar o gráfico: {e}")

# --- Função Principal (Versão NP) ---
def main_np():
    """Função principal para execução da versão não paralela."""
    inicio_np = time.time()
    print("--- Iniciando Versão Não Paralela (NP) ---")

    # Mover arquivo de exemplo (se necessário)
    arquivo_exemplo_origem = "/home/ubuntu/upload/teste_STJ.csv"
    arquivo_exemplo_destino = os.path.join(DIRETORIO_ENTRADA, "teste_STJ.csv")
    os.makedirs(DIRETORIO_ENTRADA, exist_ok=True)
    if os.path.exists(arquivo_exemplo_origem) and not os.path.exists(arquivo_exemplo_destino):
        try:
            os.rename(arquivo_exemplo_origem, arquivo_exemplo_destino)
            print(f"Arquivo de exemplo movido para {DIRETORIO_ENTRADA}")
        except OSError as e:
            print(f"Erro ao mover arquivo de exemplo: {e}")
    elif not os.path.exists(arquivo_exemplo_destino):
         print(f"Aviso: Arquivo de exemplo {arquivo_exemplo_destino} não encontrado.")

    # Etapa 1: Extração e Concatenação
    df_completo = extrair_e_concatenar_csv(DIRETORIO_ENTRADA)
    if df_completo.empty:
        print("Processamento interrompido: Nenhum dado para processar.")
        return

    # Salvar arquivo consolidado
    try:
        print(f"Salvando DataFrame consolidado em: {ARQUIVO_CONSOLIDADO}")
        # Usar separador vírgula para consistência com a leitura
        df_completo.to_csv(ARQUIVO_CONSOLIDADO, sep=",", index=False, encoding="utf-8")
        print(" -> Salvo com sucesso.")
    except Exception as e:
        print(f"Erro ao salvar o arquivo consolidado: {e}")

    # Etapa 2: Limpeza e Pré-processamento
    df_processado = limpar_e_preparar_dados(df_completo)

    # Etapa 3: Cálculo das Metas
    df_resultados_metas = calcular_todas_metas(df_processado)

    # Etapa 4: Geração de Saídas
    if not df_resultados_metas.empty:
        gerar_saidas(df_resultados_metas)
    else:
        print("Nenhum resultado de meta para gerar saídas.")

    fim_np = time.time()
    tempo_total_np = fim_np - inicio_np
    print(f"--- Versão Não Paralela (NP) Concluída em {tempo_total_np:.4f} segundos ---")

    # Guardar tempo para cálculo do speedup
    try:
        with open(ARQUIVO_TEMPO_NP, "w") as f:
            f.write(str(tempo_total_np))
        print(f"Tempo de execução NP salvo em {ARQUIVO_TEMPO_NP}")
    except Exception as e:
        print(f"Erro ao salvar tempo de execução NP: {e}")

# --- Execução ---
if __name__ == "__main__":
    main_np()

