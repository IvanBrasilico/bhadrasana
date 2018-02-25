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