 ![png-transparent-ntt-data-hd-logo-removebg-preview](https://github.com/user-attachments/assets/2a3cec06-2f52-4b7b-82fa-5dba30538834)

# Projeto NTTdata Saúde – Pipeline com Databricks

Este projeto realiza o tratamento, visualização e análise de dados simulados de saúde utilizando Apache Spark com Databricks.

## Etapas incluídas
- Leitura de dados brutos--> (🥉camada Bronze)
- Tratamento e padronização--> (🥈camada Silver)
- Visualização com matplotlib(exames e cirurgias) e Databricks UI(atendimentos)--> (🥇camada Gold)

## Visualizações geradas para Exames e Cirurgias
- Top 10 exames, cirurgias, doenças e hospitais
- Evolução mensal de exames
- Cruzamento entre exames e cirurgias por paciente

## Visualizações geradas para os dados de Atendimentos

Com foco específico nos dados de atendimentos, as seguintes visualizações foram desenvolvidas na camada Gold:

- Distribuição de Atendimentos por Local: Gráfico de barras ou pizza para entender a proporção de atendimentos em diferentes locais de atendimento, após um processo de limpeza e padronização dos nomes.
- Evolução Mensal de Atendimentos: Gráfico de linha que exibe a tendência e o volume de atendimentos ao longo dos meses e anos, permitindo identificar padrões.
- Atendimentos por Médico: Gráfico de barras que mostra o desempenho dos médicos com base no total de atendimentos realizados, identificando os profissionais com maior volume de serviço.
