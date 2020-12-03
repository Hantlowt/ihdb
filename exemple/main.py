from ihdb import Ihdb
db = Ihdb('db')
Toto = db.create_node('Person', {'name': 'toto'})
Tata = db.create_node('Person', {'name': 'tata'})
db.add_index('Person', 'name')
Toto['friendWith'] = Tata