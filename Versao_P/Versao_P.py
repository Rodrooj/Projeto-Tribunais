# -*- coding: utf-8 -*-
"""
Versão Paralela (P) com Threading para processamento de dados de tribunais.

Este script realiza o processo ETL (Extract, Transform, Load) nos arquivos CSV
disponibilizados, calcula as metas de desempenho dos tribunais de forma paralela
usando threading, conforme especificado no documento TP06, e gera arquivos
consolidados e de resumo.
"""

import pandas as pd
import os
import glob
import time
import numpy as np
import warnings
import matplotlib.pyplot as plt
import threading
import queue

# Ignorar warnings específicos do Pandas que podem ocorrer durante conversões ou divisões por zero
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

# --- Configurações Iniciais ---
DIRETORIO_ENTRADA = "/Users/rodrigo/Documents/Projetos VS Code/Projeto-Tribunais/Dados" # Diretório onde os CSVs de entrada estão
ARQUIVO_CONSOLIDADO = "/Users/rodrigo/Documents/Projetos VS Code/Projeto-Tribunais/Versao_PConsolidado.csv" # Gerado pela versão NP ou P
ARQUIVO_RESUMO_METAS = "/Users/rodrigo/Documents/Projetos VS Code/Projeto-Tribunais/Versao_P/ResumoMetas_P.csv" # Saída específica da versão P
ARQUIVO_GRAFICO = "/Users/rodrigo/Documents/Projetos VS Code/Projeto-Tribunais/Versao_P/grafico_meta1_P.png" # Saída específica da versão P
ARQUIVO_RELATORIO_SPEEDUP = "/Users/rodrigo/Documents/Projetos VS Code/Projeto-Tribunais/Versao_P/speedup_report.pdf"
ARQUIVO_TEMPO_P = "/Users/rodrigo/Documents/Projetos VS Code/Projeto-Tribunais/Versao_P/tempo_p.txt" # Para guardar tempo para speedup
NUM_THREADS = 4 # Número de threads a serem usadas (ajustar conforme necessário)

# Colunas que precisam ser numéricas para os cálculos das metas (mesma lista da NP)
COLUNAS_NUMERICAS = [
    # ... (lista completa como na Versao_NP.py) ...
    'casos_novos_2025', 'julgados_2025', 'suspensos_2025', 'dessobrestados_2025',
    'distm2_a', 'julgm2_a', 'suspm2_a',
    'distm2_b', 'julgm2_b', 'suspm2_b',
    'distm2_c', 'julgm2_c', 'suspm2_c',
    'distm2_ant', 'julgm2_ant', 'suspm2_ant',
    'distm4_a', 'julgm4_a', 'suspm4_a',
    'distm4_b', 'julgm4_b', 'suspm4_b',
    'distm6', 'julgm6', 'suspm6',
    'distm6_a', 'julgm6_a', 'suspm6_a',
    'distm7_a', 'julgm7_a', 'suspm7_a',
    'distm7_b', 'julgm7_b', 'suspm7_b',
    'distm8', 'julgm8', 'suspm8',
    'distm8_a', 'julgm8_a', 'suspm8_a',
    'distm8_b', 'julgm8_b', 'suspm8_b',
    'distm10', 'julgm10', 'suspm10',
    'distm10_a', 'julgm10_a', 'suspm10_a',
    'distm10_b', 'julgm10_b', 'suspm10_b'
]

# --- Funções Auxiliares (inalteradas) ---
def safe_division(numerador, denominador, fator=100):
    if pd.isna(denominador) or denominador == 0:
        return 0
    if pd.isna(numerador):
        return 0
    return (numerador / denominador) * fator

def get_sum(df, coluna):
    return df[coluna].sum() if coluna in df.columns else 0

# --- Etapa 1: Extração e Concatenação (igual à NP) ---
def extrair_e_concatenar_csv(diretorio):
    # (Código idêntico ao Versao_NP.py)
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
            df_temp = pd.read_csv(arquivo, sep=",", encoding="latin1", dtype=str, low_memory=False)
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
    print(f"Colunas após concatenação: {df_consolidado.columns.tolist()}")
    return df_consolidado

# --- Etapa 2: Limpeza e Pré-processamento (igual à NP) ---
def limpar_e_preparar_dados(df):
    # (Código idêntico ao Versao_NP.py corrigido)
    print("Iniciando limpeza e pré-processamento...")
    df_processado = df.copy()
    colunas_presentes = [col for col in COLUNAS_NUMERICAS if col in df_processado.columns]
    print(f"Colunas a serem convertidas para numérico: {len(colunas_presentes)}")
    if 'sigla_tribunal' not in df_processado.columns:
        print("ALERTA: Coluna 'sigla_tribunal' não encontrada ANTES da conversão numérica.")
    for coluna in colunas_presentes:
        if df_processado[coluna].dtype == 'object':
            df_processado[coluna] = df_processado[coluna].str.strip().str.replace(',', '.', regex=False)
        df_processado[coluna] = pd.to_numeric(df_processado[coluna], errors='coerce')
    print("Preenchendo valores NaN nas colunas numéricas com 0 para cálculo.")
    df_processado[colunas_presentes] = df_processado[colunas_presentes].fillna(0)
    if 'sigla_tribunal' not in df_processado.columns:
        print("ALERTA: Coluna 'sigla_tribunal' não encontrada DEPOIS da conversão numérica.")
    else:
        print("Coluna 'sigla_tribunal' presente após limpeza.")
    print("Limpeza e pré-processamento concluídos.")
    return df_processado

# --- Etapa 3: Cálculo das Metas (Função base igual à NP) ---
def calcular_meta1(grupo):
    # (Código idêntico ao Versao_NP.py)
    julgados = get_sum(grupo, 'julgados_2025')
    casos_novos = get_sum(grupo, 'casos_novos_2025')
    dessobrestados = get_sum(grupo, 'dessobrestados_2025')
    suspensos = get_sum(grupo, 'suspensos_2025')
    denominador = casos_novos + dessobrestados - suspensos
    return safe_division(julgados, denominador, fator=100)

def calcular_metas_tribunal(grupo):
    # (Código idêntico ao Versao_NP.py, com lógica para STJ e placeholder para outros)
    resultados = {}
    if grupo.empty:
        return {"sigla_tribunal": "VAZIO", "ramo_justica": "VAZIO"}
    sigla = grupo['sigla_tribunal'].iloc[0]
    ramo = grupo['ramo_justica'].iloc[0]
    resultados['sigla_tribunal'] = sigla
    resultados['ramo_justica'] = ramo
    resultados['Meta1'] = calcular_meta1(grupo)
    if ramo == 'Superior Tribunal de Justiça':
        julgados = get_sum(grupo, 'julgm2_ant'); distribuidos = get_sum(grupo, 'distm2_ant'); suspensos = get_sum(grupo, 'suspm2_ant'); denominador = distribuidos - suspensos
        resultados['Meta2ANT'] = safe_division(julgados, denominador, fator=100)
        julgados = get_sum(grupo, 'julgm4_a'); distribuidos = get_sum(grupo, 'distm4_a'); suspensos = get_sum(grupo, 'suspm4_a'); denominador = distribuidos - suspensos
        resultados['Meta4A'] = safe_division(julgados, denominador, fator=(1000/9))
        julgados = get_sum(grupo, 'julgm4_b'); distribuidos = get_sum(grupo, 'distm4_b'); suspensos = get_sum(grupo, 'suspm4_b'); denominador = distribuidos - suspensos
        resultados['Meta4B'] = safe_division(julgados, denominador, fator=100)
        julgados = get_sum(grupo, 'julgm6_a'); distribuidos = get_sum(grupo, 'distm6_a'); suspensos = get_sum(grupo, 'suspm6_a'); denominador = distribuidos - suspensos
        resultados['Meta6'] = safe_division(julgados, denominador, fator=(1000/7.5))
        julgados = get_sum(grupo, 'julgm7_a'); distribuidos = get_sum(grupo, 'distm7_a'); suspensos = get_sum(grupo, 'suspm7_a'); denominador = distribuidos - suspensos
        resultados['Meta7A'] = safe_division(julgados, denominador, fator=(1000/7.5))
        julgados = get_sum(grupo, 'julgm7_b'); distribuidos = get_sum(grupo, 'distm7_b'); suspensos = get_sum(grupo, 'suspm7_b'); denominador = distribuidos - suspensos
        resultados['Meta7B'] = safe_division(julgados, denominador, fator=(1000/7.5))
        julgados = get_sum(grupo, 'julgm8'); distribuidos = get_sum(grupo, 'distm8'); suspensos = get_sum(grupo, 'suspm8'); denominador = distribuidos - suspensos
        resultados['Meta8'] = safe_division(julgados, denominador, fator=(1000/10))
        julgados = get_sum(grupo, 'julgm10'); distribuidos = get_sum(grupo, 'distm10'); suspensos = get_sum(grupo, 'suspm10'); denominador = distribuidos - suspensos
        resultados['Meta10'] = safe_division(julgados, denominador, fator=(1000/10))
    # TODO: Implementar lógica para os outros ramos
    return resultados

# --- Etapa 3: Cálculo das Metas (Versão Paralela com Threading) ---
def worker_calcular_metas(grupo_info, results_queue):
    """Função executada por cada thread para calcular metas de um tribunal."""
    nome_grupo, grupo_df = grupo_info
    try:
        # print(f"Thread {threading.current_thread().name} processando: {nome_grupo}") # Debug
        resultado_tribunal = calcular_metas_tribunal(grupo_df)
        results_queue.put(resultado_tribunal)
    except Exception as e:
        print(f"Erro na thread processando {nome_grupo}: {e}")
        # Colocar um resultado de erro na fila pode ser útil
        results_queue.put({"sigla_tribunal": nome_grupo, "ramo_justica": "ERRO", "error": str(e)})

def calcular_todas_metas_paralelo(df):
    """Agrupa por tribunal e calcula as metas em paralelo usando threads."""
    print(f"Calculando metas por tribunal em paralelo com {NUM_THREADS} threads...")
    if 'sigla_tribunal' not in df.columns:
        print("Erro: Coluna 'sigla_tribunal' não encontrada para agrupamento.")
        print(f"Colunas disponíveis: {df.columns.tolist()}")
        return pd.DataFrame()

    grupos = list(df.groupby('sigla_tribunal')) # Materializa os grupos
    resultados_lista = []
    results_queue = queue.Queue()
    threads = []
    total_grupos = len(grupos)

    print(f"Total de tribunais para processar: {total_grupos}")

    # Cria e inicia as threads
    for i in range(total_grupos):
        grupo_info = grupos[i]
        thread = threading.Thread(target=worker_calcular_metas, args=(grupo_info, results_queue))
        threads.append(thread)
        thread.start()
        # Limitar o número de threads ativas simultaneamente (opcional, mas bom para muitos grupos)
        if len(threads) >= NUM_THREADS:
            # Espera a thread mais antiga terminar antes de iniciar a próxima
            t = threads.pop(0)
            t.join()

    # Espera as threads restantes terminarem
    for t in threads:
        t.join()

    print("Todas as threads concluíram. Coletando resultados...")

    # Coleta resultados da fila
    while not results_queue.empty():
        resultados_lista.append(results_queue.get())

    if not resultados_lista:
        print("Nenhum resultado de meta coletado das threads.")
        return pd.DataFrame()

    df_resultados = pd.DataFrame(resultados_lista)
    # Verificar se houve erros
    if 'error' in df_resultados.columns:
        erros = df_resultados[df_resultados['ramo_justica'] == 'ERRO']
        if not erros.empty:
            print(f"ALERTA: {len(erros)} tribunais tiveram erro no processamento:")
            print(erros[['sigla_tribunal', 'error']])
            # Filtrar erros para o resultado final?
            df_resultados = df_resultados[df_resultados['ramo_justica'] != 'ERRO'].copy()

    print("Cálculo de metas paralelo concluído.")
    return df_resultados

# --- Etapa 4: Geração de Saídas (igual à NP, mas com nomes de arquivo diferentes) ---
def gerar_saidas(df_resultados):
    """Gera o arquivo ResumoMetas_P.csv e o gráfico comparativo."""
    print("Gerando arquivos de saída (Versão Paralela)...")

    # 4.1: Salvar ResumoMetas_P.csv
    try:
        if not df_resultados.empty and 'sigla_tribunal' in df_resultados.columns:
             df_resumo = df_resultados.round(2).fillna('NA')
             df_resumo['sigla_tribunal'] = df_resumo['sigla_tribunal'].replace('NA', 'Desconhecido')
             df_resumo['ramo_justica'] = df_resumo['ramo_justica'].replace('NA', 'Desconhecido')
             print(f"Salvando resumo das metas em: {ARQUIVO_RESUMO_METAS}")
             df_resumo.to_csv(ARQUIVO_RESUMO_METAS, sep=";", index=False, encoding="utf-8")
             print(" -> Resumo (P) salvo com sucesso.")
        elif df_resultados.empty:
             print("DataFrame de resultados vazio, não é possível gerar resumo.")
        else:
             print("Erro: Coluna 'sigla_tribunal' ausente no DataFrame de resultados.")
    except Exception as e:
        print(f"Erro ao salvar o arquivo {ARQUIVO_RESUMO_METAS}: {e}")

    # 4.2: Gerar Gráfico Comparativo
    try:
        if not df_resultados.empty and 'Meta1' in df_resultados.columns and 'ramo_justica' in df_resultados.columns:
            meta1_validos = df_resultados.dropna(subset=['Meta1'])
            meta1_validos = meta1_validos[meta1_validos['Meta1'] != 0]
            if not meta1_validos.empty:
                media_meta1_ramo = meta1_validos.groupby('ramo_justica')['Meta1'].mean().sort_values()
                plt.figure(figsize=(12, 7))
                bars = plt.barh(media_meta1_ramo.index, media_meta1_ramo.values, color='lightcoral') # Cor diferente
                plt.xlabel('Cumprimento Médio da Meta 1 (%)')
                plt.ylabel('Ramo da Justiça')
                plt.title('Desempenho Médio da Meta 1 por Ramo da Justiça (Versão Paralela)')
                plt.grid(axis='x', linestyle='--', alpha=0.7)
                plt.tight_layout()
                for bar in bars:
                    plt.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2, f'{bar.get_width():.1f}%', va='center', ha='left', fontsize=9)
                print(f"Salvando gráfico comparativo em: {ARQUIVO_GRAFICO}")
                plt.savefig(ARQUIVO_GRAFICO)
                plt.close()
                print(" -> Gráfico (P) salvo com sucesso.")
            else:
                print("Não há dados válidos de Meta 1 para gerar o gráfico (P).")
        elif df_resultados.empty:
             print("DataFrame de resultados vazio, não é possível gerar gráfico (P).")
        else:
            print("Colunas 'Meta1' ou 'ramo_justica' não encontradas para gerar o gráfico (P).")
    except Exception as e:
        print(f"Erro ao gerar o gráfico (P): {e}")

# --- Função Principal (Versão P) ---
def main_p():
    """Função principal para execução da versão paralela com threading."""
    inicio_p = time.time()
    print("--- Iniciando Versão Paralela (P) com Threading ---")

    # A leitura e limpeza podem ser feitas sequencialmente, pois o gargalo
    # esperado é no cálculo das metas por grupo.
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

    # Salvar consolidado (opcional na versão P, já que NP gera)
    # try:
    #     print(f"Salvando DataFrame consolidado em: {ARQUIVO_CONSOLIDADO}")
    #     df_completo.to_csv(ARQUIVO_CONSOLIDADO, sep=",", index=False, encoding="utf-8")
    #     print(" -> Salvo com sucesso.")
    # except Exception as e:
    #     print(f"Erro ao salvar o arquivo consolidado: {e}")

    # Etapa 2: Limpeza e Pré-processamento
    df_processado = limpar_e_preparar_dados(df_completo)

    # Etapa 3: Cálculo das Metas (Paralelo)
    df_resultados_metas = calcular_todas_metas_paralelo(df_processado)

    # Etapa 4: Geração de Saídas
    if not df_resultados_metas.empty:
        gerar_saidas(df_resultados_metas)
    else:
        print("Nenhum resultado de meta para gerar saídas.")

    fim_p = time.time()
    tempo_total_p = fim_p - inicio_p
    print(f"--- Versão Paralela (P) Concluída em {tempo_total_p:.4f} segundos ---")

    # Guardar tempo para cálculo do speedup
    try:
        with open(ARQUIVO_TEMPO_P, "w") as f:
            f.write(str(tempo_total_p))
        print(f"Tempo de execução P salvo em {ARQUIVO_TEMPO_P}")
    except Exception as e:
        print(f"Erro ao salvar tempo de execução P: {e}")

# --- Execução ---
if __name__ == "__main__":
    main_p()

