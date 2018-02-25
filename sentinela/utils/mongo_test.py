from pymongo import MongoClient


from ajna_commons.flask.conf import MONGODB_URI, DATABASE
filtro = {'$or':
          [{'alimento': {'$in': ['bacon', 'coxinha', 'frutose', 'dextrose de milho']}},
           {'esporte': {'$in': ['basejump', 'surf', 'ufc', 'escalada']}}]}


conn = MongoClient(host=MONGODB_URI)
db = conn[DATABASE]
mongo_list = db['Conhecimento.csv'].find()
print(mongo_list[0].keys())
mongo_list = db['Container.csv'].find()
print(mongo_list[0].keys())


from sentinela.models.models import Visao, Tabela, Coluna, MySession, Base

def juncao():
    mysession = MySession(Base, test=False).session
    db = MongoClient()['test']
    visao = mysession.query(Visao).filter(Visao.id == 1).first()
    print(visao.tabelas)
    for tabela in visao.tabelas:
        print(tabela.csv)
    numero_juncoes = len(visao.tabelas)
    tabela = visao.tabelas[numero_juncoes - 1]
    print(tabela.csv)
    collection_name = 'CARGA' + '.' + tabela.csv[:-4]

    # filhofilename = os.path.join(path, tabela.csv)
    # dffilho = pd.read_csv(filhofilename, encoding=ENCODE, dtype=str)
    lista = list(db[collection_name].find())
    print(lista[:10])


juncao()


def dummy():
    if hasattr(tabela, 'type'):
        how = tabela.type
    else:
        how = 'inner'
    # print(tabela.csv, tabela.estrangeiro, tabela.primario)
    # A primeira precisa ser "pulada", sempre é a junção 2 tabelas
    # de cada vez. Se numero_juncoes for >2, entrará aqui fazendo
    # a junção em cadeia desde o último até o primeiro filho
    for r in range(numero_juncoes - 2, 0, -1):
        paifilhofilename = os.path.join(path, visao.tabelas[r].csv)
        dfpaifilho = pd.read_csv(paifilhofilename, encoding=ENCODE,
                                 dtype=str)
        # print(tabela.csv, tabela.estrangeiro, tabela.primario)
        dffilho = dfpaifilho.merge(dffilho, how=how,
                                   left_on=tabela.primario.lower(),
                                   right_on=tabela.estrangeiro.lower())
        tabela = visao.tabelas[r]
        paifilhofilename = os.path.join(path, tabela.csv)
        if hasattr(tabela, 'type'):
            how = tabela.type
        else:
            how = 'inner'
    csv_pai = visao.tabelas[0].csv
    paifilename = os.path.join(path, csv_pai)
    dfpai = pd.read_csv(paifilename, encoding=ENCODE, dtype=str)
    dfpai = dfpai.merge(dffilho, how=how,
                        left_on=tabela.primario.lower(),
                        right_on=tabela.estrangeiro.lower())
    if visao.colunas:
        colunas = [coluna.nome.lower() for coluna in visao.colunas]
        result_df = dfpai[colunas]
        result_list = [colunas]
    else:
        result_df = dfpai
        result_list = [result_df.columns.tolist()]
    result_list.extend(result_df.values.tolist())
    # parametros_ativos=parametros_ativos)
    return result_list


db['Conhecimento.csv'].aggregate([
  { '$unwind': "$containeres" },
  { 
    '$lookup': 
      { 
        'from': "Container.csv", 
        'localField': "conhecimento", 
        'foreignField': "conhecimento", 
        'as': "container"
      }
  },
  { '$unwind': "$container" },
  { 
    '$group': 
      { 
        "conhecimento": "$containeres",
        "conhecimento": { '$first': "$conhecimento.conhecimento" }, 
        "container": { '$push': { 'dep_id': "$_id", 'dep_name': "$dep_name" } } 
      } 
  } 
])