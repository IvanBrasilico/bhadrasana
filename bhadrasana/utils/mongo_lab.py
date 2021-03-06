"""Arquivo temporário para testes exploratórios com novos métodos MongoDB."""
from pymongo import MongoClient

from bhadrasana.models.models import Base, MySession, PadraoRisco, Visao
from bhadrasana.utils.gerente_risco import GerenteRisco

gerente = GerenteRisco()
mysession = MySession(Base, test=False).session
db = MongoClient()['test']
visao = mysession.query(Visao).filter(Visao.id == 1).first()
padrao = mysession.query(PadraoRisco).filter(PadraoRisco.id == 1).first()
gerente.set_padraorisco(padrao)
df = gerente.aplica_juncao_mongo(db, visao,
                                 parametros_ativos=['cpfcnpjconsignatario'],
                                 filtrar=True, pandas=False)
print(df)


filtro = {'$or':
          [{'alimento': {'$in': ['bacon', 'coxinha',
                                 'frutose', 'dextrose de milho']}},
           {'esporte': {'$in': ['basejump', 'surf', 'ufc', 'escalada']}}]}


"""
db['Conhecimento.csv'].aggregate([
    {'$unwind': "$containeres"},
    {
        '$lookup':
        {
            'from': "Container.csv",
            'localField': "conhecimento",
            'foreignField': "conhecimento",
            'as': "container"
        }
    },
    {'$unwind': "$container"},
    {
        '$group':
        {
            "conhecimento": "$containeres",
            "conhecimento": {'$first': "$conhecimento.conhecimento"},
            "container": {'$push': {'dep_id': "$_id", 'dep_name': "$dep_name"}}
        }
    }
])
"""
