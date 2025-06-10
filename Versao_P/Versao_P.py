import pandas as pd
import os
from multiprocessing import Pool
import matplotlib.pyplot as plt
import seaborn as sns
import time
import logging
import tempfile
import shutil

# Configura o logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# Meta 1: Julgar mais processos que os distribuídos.
# Fórmula: (∑ julgadom1 / (∑ cnm1 + ∑ desm1 - ∑ susm1)) * 100

# Meta 2A: Identificar e julgar pelo menos 95% dos processos.
# Fórmula: (∑ julgadom2_a / (∑ distm2_a - ∑ suspm2_a)) * (1000 / 9.5)

# Meta 2B: Identificar e julgar pelo menos 100% dos processos.
# Fórmula: (∑ julgadom2_b / (∑ distm2_b - ∑ suspm2_b)) * 100

# Meta 2C: Identificar e julgar pelo menos 95% dos processos.
# Fórmula: (∑ julgadom2_c / (∑ distm2_c - ∑ suspm2_c)) * (1000 / 9.5)

# Meta 2ANT: Identificar e julgar pelo menos 100% dos processos.
# Fórmula: (∑ julgadom2_ant / (∑ distm2_ant - ∑ susm2_ant)) * 100

# Meta 4A: Identificar e julgar pelo menos 95% ddos processos.
# Fórmula: (∑ julgadom4_a / (∑ distm4_a - ∑ suspm4_a)) * (1000 / 6.5)

# Meta 4B: Identificar e julgar 100% dos processos.
# Fórmula: (∑ julgadom4_b / (∑ dism4_b - ∑ susm4_b)) * (100)

# Meta 6: Julgar 75% dos processos.
# Fórmula: (∑ julgadom6 / (∑ dism6 - ∑ susm6)) * (1000 / 5)

# Meta 7A: Julgar 75% dos processos.
# Fórmula: (∑ julgadom7_a / (∑ dism7_a - ∑ susm7_a)) * (1000/5)

# Meta 7B: Julgar 75% dos processos.
# Fórmula: (∑ julgadom7_b / (∑ dism7_b - ∑ susm7_b)) * (1000/5)

# Meta 8A: Identificar e julgar 75% dos processos.
# Fórmula: (∑ julgadom8_a / (∑ dism8_a - ∑ susm8_a)) * (1000/7,5)

# Meta 8B: Identificar e julgar 100% dos processos.
# Fórmula: (∑ julgadom8_b / (∑ dism8_b - ∑ suspm8_b)) * (1000/9)

# Meta 10A: Identificar e julgar 90% dos processos.
# Fórmula: (∑ julgadom10_a / (∑ dism10_a - ∑ susm10_a)) * (1000/9)

# Meta 10B: Identificar e julgar 100% dos processos.
# Fórmula: (∑ julgadom10_b / (∑ dism10_b - ∑ susm10_b)) * (1000/10)

def calculate_meta1(df):
    cnm1 = df["casos_novos_2025"].sum() if "casos_novos_2025" in df.columns else 0
    julgadom1 = df["julgados_2025"].sum() if "julgados_2025" in df.columns else 0
    desm1 = df["dessobrestados_2025"].sum() if "dessobrestados_2025" in df.columns else 0
    susm1 = df["suspensos_2025"].sum() if "suspensos_2025" in df.columns else 0
    denominator = cnm1 + desm1 - susm1
    if denominator == 0:
        return 0
    return (julgadom1 / denominator) * 100


def calculate_meta_generic(df, julgado_col, dist_col, susp_col, target_percentage):
    julgado = df[julgado_col].sum() if julgado_col in df.columns else 0
    dist = df[dist_col].sum() if dist_col in df.columns else 0
    susp = df[susp_col].sum() if susp_col in df.columns else 0
    denominator = dist - susp
    if denominator == 0:
        return 0
    return (julgado / denominator) * (1000 / target_percentage)


def process_csv(file_path):
    try:
        logging.info(f"Iniciando processamento do arquivo: {file_path}")
        print(f"Processando arquivo: {os.path.basename(file_path)}")  # Printa o arquivo que está sendo lido
        df = pd.read_csv(file_path)
        logging.info(f"Arquivo {file_path} lido com sucesso.")

        # Obtém o nome do tribunal
        tribunal_name = df["sigla_tribunal"].iloc[0] if "sigla_tribunal" in df.columns and not df.empty else "UNKNOWN"

        # Calcula as metas
        meta1 = calculate_meta1(df)
        meta2a = calculate_meta_generic(df, "julgm2_a", "distm2_a", "suspm2_a", 9.5)
        meta2b = calculate_meta_generic(df, "julgm2_b", "distm2_b", "suspm2_b", 100)
        meta2c = calculate_meta_generic(df, "julgm2_c", "distm2_c", "suspm2_c", 9.5)
        meta2ant = calculate_meta_generic(df, "julgm2_ant", "distm2_ant", "susm2_ant",100)
        meta4a = calculate_meta_generic(df, "julgm4_a", "distm4_a", "suspm4_a", 9.5)
        meta4b = calculate_meta_generic(df, "julgm4_b", "dism4_b", "susm4_b", 100)
        meta6 = calculate_meta_generic(df, "julgadom6", "dism6", "susm6", 7.5)
        meta7a = calculate_meta_generic(df, "julgadom7_a", "dism7_a", "susm7_a", 7.5)
        meta7b = calculate_meta_generic(df, "julgadom7_b", "dism7_b", "susm7_b", 7.5)
        meta8a = calculate_meta_generic(df, "julgadom8_a", "dism8_a", "susm8_a", 7.5)
        meta8b = calculate_meta_generic(df, "julgadom8_b", "dism8_b", "suspm8_b", 100)
        meta10a = calculate_meta_generic(df, "julgadom10_a", "dism10_a", "susm10_a", 9)
        meta10b = calculate_meta_generic(df, "julgadom10_b", "dism10_b", "susm10_b", 100)

        logging.info(f"Metas calculadas para o arquivo: {file_path}")

        # Salva o DataFrame bruto em memória temporária
        temp_raw_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        df.to_csv(temp_raw_file, index=False)
        logging.info(f"Dados brutos de {file_path} salvos temporariamente em {temp_raw_file}")

        # Retorna DataFrame com os resultados das metas
        return (
            pd.DataFrame({
                "sigla_tribunal": [tribunal_name],
                "meta1_calculated": [meta1],
                "meta2a_calculated": [meta2a],
                "meta2b_calculated": [meta2b],
                "meta2c_calculated": [meta2c],
                "meta2ant_calculated": [meta2ant],
                "meta4a_calculated": [meta4a],
                "meta4b_calculated": [meta4b],
                "meta6_calculated": [meta6],
                "meta7a_calculated": [meta7a],
                "meta7b_calculated": [meta7b],
                "meta8a_calculated": [meta8a],
                "meta8b_calculated": [meta8b],
                "meta10a_calculated": [meta10a],
                "meta10b_calculated": [meta10b]
            }),
            temp_raw_file  # Retorna o caminho do arquivo temporario
        )
    except Exception as e:
        logging.error(f"Erro ao processar o arquivo {file_path}: {e}")
        return None, None  # Retorna nulo para ambos em caso de erro


def main():
    input_dir = "./Dados"  # Pasta de input dos arquivos CSVs
    output_dir = "./results" # Pasta de output dos resultados e do grafico
    os.makedirs(output_dir, exist_ok=True)

    csv_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".csv")]

    # Usa todos os cores do processador disponíveis para a paralelização
    num_processes = os.cpu_count()
    print(f"Processando {len(csv_files)} arquivos CSV usando {num_processes} processos...")

    start_time = time.time()

    with Pool(num_processes) as pool:
        # processed_results é uma lista de tuplas: (aggregated_df, temp_raw_file_path)
        processed_results = pool.map(process_csv, csv_files)

    # Filtra valores nulos de processos falhos e separa os Dataframes e arquivos temporários
    aggregated_dfs = []
    temp_raw_file_paths = []
    for agg_df, temp_path in processed_results:
        if agg_df is not None and temp_path is not None:
            aggregated_dfs.append(agg_df)
            temp_raw_file_paths.append(temp_path)

    if not aggregated_dfs:
        print("Nenhum arquivo CSV foi processado com sucesso.")
        return

    # Gera o arquivo ResumoMetas.CSV
    concatenated_aggregated_df = pd.concat(aggregated_dfs, ignore_index=True)
    concatenated_aggregated_df.to_csv(os.path.join(output_dir, "ResumoMetas.CSV"), index=False)
    print(f"Resumo das metas salvo em {os.path.join(output_dir, 'ResumoMetas.CSV')}")

    # Gera o arquivo Consolidado.csv
    consolidated_output_path = os.path.join(output_dir, "Consolidado.csv")
    if temp_raw_file_paths:
        with open(consolidated_output_path, 'w') as outfile:
            # Escreve o cabeçalho das colunas usando o primeiro arquivo como base
            with open(temp_raw_file_paths[0], 'r') as infile:
                outfile.write(infile.readline())
            # Junta o conteúdo dos arquivos subsequentes (pulando o cabeçalho)
            for i, temp_path in enumerate(temp_raw_file_paths):
                with open(temp_path, 'r') as infile:
                    if i == 0: # Pula a linha de cabeçalho no primeiro arquivo, já está escrita
                        next(infile)
                    shutil.copyfileobj(infile, outfile) # Cópia em bloco otimizada
        print(f"Arquivo consolidado salvo em {consolidated_output_path}")
    else:
        print("Nenhum dado bruto foi processado para consolidação.")

    end_time = time.time()
    print(f"Tempo total de execução: {end_time - start_time:.2f} segundos")

    # Limpa os arquivos temporários
    for temp_path in temp_raw_file_paths:
        os.remove(temp_path)

    # Gera o gráfico
    # Agrega dados para melhor legibilidade
    if "sigla_tribunal" in concatenated_aggregated_df.columns and "meta1_calculated" in concatenated_aggregated_df.columns:
        aggregated_df_for_plot = concatenated_aggregated_df.groupby("sigla_tribunal")[
            "meta1_calculated"].mean().reset_index()
        aggregated_df_for_plot = aggregated_df_for_plot.sort_values(by="meta1_calculated", ascending=False)

        plt.figure(figsize=(14, 7))
        sns.barplot(x="sigla_tribunal", y="meta1_calculated", data=aggregated_df_for_plot)
        plt.title("Cumprimento Médio da Meta 1 por Tribunal")
        plt.xlabel("Tribunal")
        plt.ylabel("Cumprimento Médio da Meta 1 (%)")
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'meta1_Tribunais.png'))
        print(f"Gráfico de cumprimento médio da Meta 1 salvo em {os.path.join(output_dir, 'meta1_Tribunais.png')}")


if __name__ == "__main__":
    main()


