# Atividade de Spark
Utilizando o dataset público das viagens de bikes de NY, crie scripts em PySpark para execução no Dataproc Serverless do GCP para responder as seguintes perguntas:

1. Crie código com PySpark para calcular a duração média das viagens e exiba as 10 mais lentas. 
2. Crie código com PySpark para calcular a duração média das viagens e exiba as 10 mais rápidas (remova as sem duração)
3. Crie código com PySpark para calcular a duração média das viagens para cada par de estações. Em seguida,  ordene-as e exiba as 10 mais mais lentas. Exemplo de saída esperada:

| start_station_id | end_station_id | duration |
| --- | --- | --- |
| 483 | 546 | 5448 |
| 483 | 546 | 5448 |
1. Crie código com PySpark para calcular o total de viagens por bicicleta e exibe as 10 bicicletas mais utilizadas
2. Crie código com PySpark para calcular a média de idade dos clientes
3. Crie código com PySpark para calcular a distribuição entre gêneros

<aside>
💡 Monte o setup seguindo o tutorial visto em aula e disponível em: https://console.cloud.google.com/welcome?walkthrough_id=dataproc--dataproc_serverless_quickstart
</aside>
