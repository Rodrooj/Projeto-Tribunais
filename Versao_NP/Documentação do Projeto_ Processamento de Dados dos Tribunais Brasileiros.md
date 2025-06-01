# Documentação do Projeto: Processamento de Dados dos Tribunais Brasileiros

## 1. Introdução

Este documento detalha o desenvolvimento de um programa em Python para processar arquivos CSV contendo dados de desempenho dos tribunais brasileiros. O objetivo principal é calcular o cumprimento das Metas Nacionais do Poder Judiciário, conforme especificado no documento "TP06 - Processando CSVs", e analisar a performance do processamento, incluindo uma versão paralela do código, seguindo as diretrizes e focos de otimização da pesquisa "CBL Projetos Tribunais".

**Documentos de Referência:**

*   `TP06-ProcessandoCSVs.pdf`: Define as regras de negócio, as fórmulas de cálculo das metas para cada ramo da justiça e os requisitos de entrega (arquivos de saída, versões paralela e não paralela, relatório de speedup, gráfico).
*   `CBLProjetoTribunais.pdf`: Pesquisa do usuário que enfatiza a necessidade de alta performance, uso eficiente de bibliotecas como Pandas, Matplotlib/Seaborn, e exploração de paralelismo (multiprocessing/threading) para otimizar operações custosas como ETL e análise.
*   `teste_STJ.csv`: Arquivo CSV de exemplo utilizado para análise inicial da estrutura dos dados.

**Arquivos Gerados pelo Projeto:**

*   `Versao_NP.py`: Código-fonte da versão sequencial (não paralela) do processador.
*   `Versao_P.py`: Código-fonte da versão paralela do processador (a ser implementada).
*   `Consolidado.csv`: Arquivo CSV contendo a concatenação de todos os arquivos CSV de entrada.
*   `ResumoMetas.csv`: Arquivo CSV com o resultado do cálculo das metas para cada tribunal.
*   `grafico_meta1.png` (ou similar): Gráfico comparativo do desempenho dos tribunais (ex: Meta 1).
*   `speedup_report.pdf`: Relatório detalhando a comparação de tempo de execução entre as versões NP e P e o speedup obtido.
*   `todo.md`: Checklist interno para acompanhamento do desenvolvimento.
*   `documentacao_projeto.md`: Este arquivo.

## 2. Estrutura do Projeto e Fluxo de Processamento (ETL)

O projeto segue um fluxo ETL (Extract, Transform, Load) adaptado aos requisitos:

1.  **Extract (Extração):** Localizar e ler todos os arquivos `.csv` de um diretório especificado (`dados_csv`). Os dados de todos os arquivos são carregados e concatenados em um único DataFrame usando a biblioteca Pandas.
2.  **Transform (Transformação):**
    *   **Limpeza e Pré-processamento:** Converter colunas relevantes para o formato numérico, tratando possíveis erros de formato (ex: vírgula como separador decimal) e valores ausentes (NaNs). Os NaNs em colunas numéricas são preenchidos com 0 para viabilizar os cálculos subsequentes.
    *   **Cálculo das Metas:** Agrupar os dados por tribunal (`sigla_tribunal`) e aplicar as fórmulas de cálculo das metas especificadas no TP06, considerando o `ramo_justica` de cada tribunal.
3.  **Load (Carga/Saída):**
    *   Salvar o DataFrame consolidado (bruto, após concatenação) em `Consolidado.csv`.
    *   Salvar os resultados dos cálculos das metas em `ResumoMetas.csv`, utilizando "NA" para indicar metas não aplicáveis ou resultados inválidos.
    *   Gerar um gráfico comparativo (ex: Meta 1) e salvá-lo como imagem.
    *   (Para a versão paralela) Medir tempos, calcular speedup e gerar `speedup_report.pdf`.

## 3. Detalhamento da Implementação (`Versao_NP.py` - Parcial)

Até o momento, as seguintes etapas foram implementadas na versão não paralela (`Versao_NP.py`):

### 3.1. Etapa 1: Extração e Concatenação

*   **Localização:** A função `glob.glob` é usada para encontrar todos os arquivos com extensão `.csv` no diretório `DIRETORIO_ENTRADA` (`/home/ubuntu/dados_csv`).
*   **Leitura:** Cada arquivo CSV é lido usando `pd.read_csv`. Parâmetros importantes foram definidos com base na análise do arquivo de exemplo e nos requisitos de robustez:
    *   `sep=";"`: Define o ponto e vírgula como separador de colunas.
    *   `encoding="latin1"`: Especifica a codificação para lidar corretamente com caracteres acentuados comuns em português.
    *   `dtype=str`: Lê todas as colunas inicialmente como texto (string). Isso evita erros de inferência de tipo do Pandas, especialmente em arquivos grandes ou com dados inconsistentes, garantindo que todos os dados sejam carregados antes da conversão controlada.
    *   `low_memory=False`: Desativa o processamento interno do Pandas em chunks, o que pode consumir mais memória, mas evita avisos de `DtypeWarning` quando há tipos mistos em colunas grandes, sendo mais robusto para a leitura inicial completa.
*   **Concatenação:** Os DataFrames individuais lidos de cada arquivo são reunidos em uma lista e, em seguida, concatenados verticalmente usando `pd.concat(lista_dfs, ignore_index=True)` para formar um único DataFrame (`df_completo`). `ignore_index=True` reinicia os índices do DataFrame resultante.
*   **Saída Consolidada:** O DataFrame `df_completo` é salvo imediatamente no arquivo `Consolidado.csv` usando `df_completo.to_csv(...)`, com separador `;` e codificação `utf-8` (recomendada para maior compatibilidade).

### 3.2. Etapa 2: Limpeza e Pré-processamento

*   **Identificação de Colunas:** Uma lista `COLUNAS_NUMERICAS` foi definida com base nas fórmulas do TP06 e nas colunas presentes no CSV de exemplo. Ela contém os nomes das colunas que precisam ser convertidas para números para permitir os cálculos das metas.
*   **Conversão Numérica:** A função `limpar_e_preparar_dados` itera sobre as `COLUNAS_NUMERICAS` presentes no DataFrame:
    1.  Remove espaços em branco (`.str.strip()`).
    2.  Substitui vírgulas por pontos (`.str.replace(',', '.', regex=False)`) para padronizar o separador decimal.
    3.  Converte a coluna para tipo numérico usando `pd.to_numeric(errors='coerce')`. O argumento `errors='coerce'` é crucial, pois transforma quaisquer valores que não possam ser convertidos em `NaN` (Not a Number), evitando que o programa pare por erro.
*   **Tratamento de NaN:** Após a conversão, quaisquer valores `NaN` (sejam os originais ou os resultantes de erros de conversão) nas colunas numéricas são preenchidos com `0` usando `.fillna(0)`. Essa abordagem foi escolhida porque a presença de `NaN` em operações aritméticas geralmente propaga o `NaN`, o que impediria o cálculo das metas. Assumir 0 para dados ausentes ou inválidos permite que o cálculo prossiga, embora possa impactar a precisão se a quantidade de dados faltantes for muito grande (uma análise mais aprofundada dos dados reais seria necessária para definir a melhor estratégia de imputação, mas 0 é um ponto de partida funcional).

## 4. Próximas Etapas da Implementação

O desenvolvimento continuará com as seguintes etapas, conforme o `todo.md`:

1.  **Cálculo das Metas (NP):** Implementar as funções para calcular cada meta definida no TP06, agrupando os dados por `sigla_tribunal` e selecionando a fórmula correta com base no `ramo_justica`.
2.  **Geração de Saídas (NP):** Criar o DataFrame `ResumoMetas` com os resultados, formatar valores ausentes como 'NA', e gerar o gráfico comparativo usando Matplotlib ou Seaborn.
3.  **Implementação da Versão Paralela (P):** Criar `Versao_P.py`, identificar os gargalos de performance (provavelmente o cálculo das metas por tribunal) e aplicar paralelismo usando `multiprocessing` para acelerar o processamento.
4.  **Medição e Relatório de Speedup:** Medir o tempo de execução das versões NP e P, calcular o speedup e gerar o relatório em PDF.
5.  **Finalização e Documentação:** Adicionar comentários finais, docstrings, refinar esta documentação e preparar os arquivos para entrega.

## 5. Como Executar (Estado Atual)

1.  Certifique-se de que os arquivos CSV dos tribunais estejam localizados no diretório `/home/ubuntu/dados_csv`.
2.  Execute o script da versão não paralela no terminal:
    ```bash
    python3.11 /home/ubuntu/Versao_NP.py
    ```
3.  Após a execução, o arquivo `Consolidado.csv` será gerado em `/home/ubuntu/`.

*(As instruções serão atualizadas conforme as próximas etapas forem implementadas)*

