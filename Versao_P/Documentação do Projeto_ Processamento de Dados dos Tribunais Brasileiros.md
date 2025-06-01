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




## 6. Versão Paralela com Threading (`Versao_P.py`)

Conforme solicitado e alinhado com a pesquisa sobre otimização, foi desenvolvida uma versão paralela do script (`Versao_P.py`) utilizando a biblioteca `threading` do Python. O objetivo principal da paralelização foi acelerar a **Etapa 3: Cálculo das Metas**, que envolve processar cada grupo de tribunal individualmente, uma tarefa inerentemente paralelizável.

### 6.1. Abordagem de Paralelização

*   **Identificação do Gargalo:** A etapa de cálculo das metas, que itera sobre cada tribunal (`groupby("sigla_tribunal")`) e aplica a função `calcular_metas_tribunal`, foi identificada como o principal ponto onde o paralelismo poderia trazer benefícios, especialmente com um grande número de tribunais (arquivos CSV).
*   **Implementação com `threading`:**
    *   A função `calcular_todas_metas_paralelo` substitui a versão sequencial.
    *   Os grupos de tribunais (obtidos com `df.groupby("sigla_tribunal")`) são primeiro materializados em uma lista.
    *   Uma `queue.Queue` é utilizada para coletar os resultados das threads de forma segura (thread-safe).
    *   Um número definido de threads (`NUM_THREADS`, configurado como 4 neste exemplo) é criado.
    *   Cada thread executa a função `worker_calcular_metas`, que recebe as informações de um grupo de tribunal (`nome_grupo`, `grupo_df`) e a fila de resultados.
    *   Dentro do `worker_calcular_metas`, a função original `calcular_metas_tribunal` é chamada para o grupo específico.
    *   O dicionário de resultados retornado por `calcular_metas_tribunal` é colocado na `results_queue`.
    *   O loop principal gerencia a criação e o início das threads, garantindo que não mais que `NUM_THREADS` estejam ativas simultaneamente (usando `join()` para esperar threads mais antigas terminarem antes de iniciar novas, se necessário).
    *   Após todas as threads serem iniciadas e concluídas (`join()`), os resultados são coletados da fila e compilados em um DataFrame final.

### 6.2. Considerações sobre Threading e o GIL

É importante notar que o `threading` em CPython (a implementação padrão do Python) é limitado pelo **Global Interpreter Lock (GIL)**. O GIL é um mutex que protege o acesso a objetos Python, impedindo que múltiplas threads executem bytecode Python *exatamente* ao mesmo tempo dentro de um único processo. 

*   **Para tarefas intensivas em CPU (CPU-bound):** Como cálculos numéricos puros, o `threading` pode não oferecer um speedup significativo (ou pode até ser mais lento devido à sobrecarga de gerenciamento de threads e contenção do GIL), pois apenas uma thread executa código Python por vez.
*   **Para tarefas intensivas em I/O (I/O-bound):** Como leitura/escrita de arquivos, operações de rede, ou espera por dispositivos externos, o `threading` é muito eficaz. Enquanto uma thread está esperando por uma operação de I/O (que libera o GIL), outra thread pode executar.

No nosso caso, embora o cálculo das metas envolva operações numéricas (CPU-bound), grande parte do tempo dentro das funções do Pandas (como `.sum()`, `.groupby()`, etc.) é gasta em código C otimizado que *pode* liberar o GIL. Portanto, algum nível de paralelismo e speedup *pode* ser observado, especialmente se houver operações de I/O implícitas ou se as operações do Pandas liberarem o GIL eficientemente. No entanto, para obter paralelismo *verdadeiro* em tarefas puramente CPU-bound em Python, a biblioteca `multiprocessing` (que cria processos separados, cada um com seu próprio interpretador Python e GIL) seria geralmente mais indicada, como mencionado na sua pesquisa.

A escolha por `threading` aqui seguiu sua solicitação explícita.

## 7. Comparação de Desempenho e Speedup

Para avaliar o ganho obtido com a paralelização, medimos o tempo total de execução das duas versões usando o arquivo de exemplo `teste_STJ.csv`.

*   **Tempo de Execução (Versão Não Paralela - NP):** 1.0859 segundos
*   **Tempo de Execução (Versão Paralela - P com Threading):** 0.8873 segundos

**Speedup:**

O speedup é calculado como a razão entre o tempo de execução da versão sequencial e o tempo de execução da versão paralela:

`Speedup = Tempo_NP / Tempo_P`
`Speedup = 1.0858650207519531 / 0.8873131275177002`
`Speedup ≈ 1.22`

**Análise:**

Neste caso, com apenas um tribunal (STJ) no arquivo de exemplo, a sobrecarga de criar e gerenciar threads quase compensou o ganho potencial. O speedup de aproximadamente 1.22x indica uma pequena melhoria. É **esperado** que o speedup seja mais significativo ao processar um número maior de arquivos CSV (e, portanto, mais tribunais), onde a paralelização do cálculo por tribunal terá um impacto maior.

*Nota: Os tempos de execução podem variar ligeiramente dependendo das condições do sistema no momento da execução.*

## 8. Como Executar

1.  **Preparação:**
    *   Certifique-se de ter Python 3 e a biblioteca Pandas instalada (`pip install pandas matplotlib`).
    *   Coloque todos os arquivos CSV dos tribunais que deseja processar no diretório `/home/ubuntu/dados_csv` (ou ajuste a variável `DIRETORIO_ENTRADA` nos scripts).
2.  **Executar Versão Não Paralela (NP):**
    ```bash
    python3.11 /home/ubuntu/Versao_NP.py
    ```
    *   **Saídas:** `Consolidado.csv`, `ResumoMetas.csv`, `grafico_meta1.png`, `tempo_np.txt`.
3.  **Executar Versão Paralela (P) com Threading:**
    ```bash
    python3.11 /home/ubuntu/Versao_P.py
    ```
    *   **Saídas:** `ResumoMetas_P.csv`, `grafico_meta1_P.png`, `tempo_p.txt`. (O `Consolidado.csv` não é regerado por padrão na versão P, pois assume-se que a versão NP já o criou).
4.  **Verificar Resultados:** Examine os arquivos CSV e a imagem do gráfico gerados em `/home/ubuntu/`.

## 9. Observações Finais e Próximos Passos (Sugestões)

*   **Implementação Completa das Metas:** O código atual calcula as metas apenas para o STJ como exemplo. É **essencial** completar a função `calcular_metas_tribunal` em ambos os scripts (`Versao_NP.py` e `Versao_P.py`) com a lógica de cálculo para **todos os outros ramos da justiça** (Estadual, Trabalho, Federal, Militar, Eleitoral, TST), conforme as fórmulas e nomes de colunas exatos definidos no TP06 e verificados nos respectivos arquivos CSV.
*   **Robustez:** Adicionar mais tratamento de erros, especialmente na leitura dos CSVs e no acesso a colunas que podem não existir em todos os arquivos.
*   **Otimização:** Explorar otimizações no Pandas (ex: uso de tipos de dados mais eficientes após a leitura, como `category` para colunas textuais com poucos valores únicos) pode reduzir o uso de memória.
*   **Relatório de Speedup:** O cálculo foi feito aqui, mas a geração do arquivo `speedup_report.pdf` ainda precisa ser implementada (pode ser feito criando um markdown e usando `manus-md-to-pdf`).
*   **Multiprocessing:** Para um ganho de performance potencialmente maior em tarefas CPU-bound, considerar implementar uma terceira versão usando a biblioteca `multiprocessing`.

