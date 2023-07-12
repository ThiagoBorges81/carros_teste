# Case "Manipulação de dados utilizando Python e MongoDB."
# Thiago Oliveira Borges

# Este teste prático faz parte de processo seletivo profissional.

#############################
#     0.1. CONTEXTO         #
#############################

# Elabore um script em Python, que deverá criar dois Pandas DataFrames, que serão populados com dados da industria automobilista, fornecidos pelo recrutador;

# Os DataFrames deverão ser enviados e salvos em um banco de dados MongoDB;

# Os dados dos dois DataFrames deverão ser agregados, unindo as duas collections, utilizando campo informado como chave para a agregação;

# Ao final do processo, salve a agregação em um arquivo do tipo .js.


#############################
# 1.0.CARREGA AS BIBLIOTECAS#
#############################

import json
import pandas as pd

from pymongo import MongoClient


##################################
# 2.0.DESENVOLVIMENTO DA SOLUÇÃO #
##################################

# 2.1. Criação dos DataFrames populados

# Dados do primeiro DataFrame
data1 = {
    'carro': ['onix', 'polo', 'sandero', 'fiesta', 'city'],
    'cor': ['prata', 'branco', 'prata', 'vermelho', 'preto'],
    'montadora': ['chevrolet', 'volkswagen', 'renault', 'ford', 'honda']
}

# Dados do segundo DataFrame
data2 = {
    'montadora': ['chevrolet', 'volkswagen', 'renault', 'ford', 'honda'],
    'pais': ['eua', 'alemanha', 'frança', 'eua', 'japao']
}

# Converter os dados para Pandas DataFrames
df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)


# 2.2. Conexão dos DataFrames com MongoDB

# Conexão com o MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['meu_banco_de_dados']

# Converter DataFrames em listas de dicionários
df1_dict = df1.to_dict('records')
df2_dict = df2.to_dict('records')

# Salvar DataFrames no MongoDB como coleções
db.primeiro_dataframe.insert_many(df1_dict)
db.segundo_dataframe.insert_many(df2_dict)

# 2.3. Agregar os dados

# Agregação: Unir as duas collections usando o campo "montadora"
pipeline = [
    {
        '$lookup': {
            'from': 'segundo_dataframe',
            'localField': 'montadora',
            'foreignField': 'montadora',
            'as': 'dados_completos'
        }
    },
    {
        '$unwind': '$dados_completos'
    },
    {
        '$project': {
            '_id': 0,
            'carro': 1,
            'cor': 1,
            'montadora': 1,
            'pais': '$dados_completos.pais'
        }
    }
]

resultado_agregacao = list(db.primeiro_dataframe.aggregate(pipeline))
print(resultado_agregacao)

# 2.4. Salvar o resultado da agregação em formato .js

# Converte o resultado da agregação para json string
res_agreg_json = json.dumps(resultado_agregacao)

# Define o nome do arquivo
carros_json = 'resultado_agregacao.js'

# Escreve o json string para o arquivo .js
with open(carros_json, 'w') as file:
    file.write('var resAgreg = ')
    file.write(res_agreg_json)

print(f'Arquivo {carros_json} salvo.')

#################
# 4.0 CONCLUSÃO #
#################

# Python e MongoDB combinados são poderosos para manipulação de dados. O desenvolvimento da solução acima demonstra a criação, transformação, e carregamento (ETL) de um conjunto de dados apresentados. O script ainda oferece a opção de preparar o resultado final e salvar o mesmo em formato .js para uso em projetos futuros.

