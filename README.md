 ![png-transparent-ntt-data-hd-logo-removebg-preview](https://github.com/user-attachments/assets/2a3cec06-2f52-4b7b-82fa-5dba30538834)

# Projeto NTTdata Saúde – Pipeline com Databricks

Este projeto realiza o tratamento, visualização e análise de dados simulados de saúde utilizando Apache Spark com Databricks.

## Etapas incluídas
- Leitura de dados brutos --> (🥉camada Bronze)
- Tratamento e padronização --> (🥈camada Silver)
- Visualização com matplotlib para as planilhas: exames , cirurgias e atendimentos --> (🥇camada Gold)

## Visualizações geradas para Exames e Cirurgias
- Top 10 exames, cirurgias, doenças e hospitais
- Evolução mensal de exames
- Cruzamento entre exames e cirurgias por paciente

## Visualizações geradas para os dados de Atendimentos

Com foco específico nos dados de atendimentos, as seguintes visualizações foram desenvolvidas na camada Gold:

- Distribuição de Atendimentos por Local
- Atendimentos por especialidade do  Médico
