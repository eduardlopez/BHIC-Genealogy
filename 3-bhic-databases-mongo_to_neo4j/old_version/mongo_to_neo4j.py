__author__ ="eduardlopez"
from pymongo import MongoClient
import py2neo as neo4j
from bson.objectid import ObjectId




def flatten_dict(d):
    def expand(key, value):
        if isinstance(value, dict):
            return [ (key + '_' + k, v) for k, v in flatten_dict(value).items() ]
        else:
            return [ (key, value) ]
    items = [ item for k, v in d.items() for item in expand(k, v) ]
    return dict(items)


############ CASE 1 ############
case_case1 = ['Bruidegom', 'Moeder van de bruidegom', 'Vader van de bruidegom', 'Vader van de bruid', 'Bruid', 'Moeder van de bruid'].sort()
translations_case1 = { 'Bruidegom':'Groom',
                 'Moeder van de bruidegom':'GroomMother',
                 'Vader van de bruidegom':'GroomFather',
                 'Vader van de bruid':'BrideFather',
                 'Bruid':'Bride',
                 'Moeder van de bruid':'BrideMother' }
# Can be in any order
relations_case1 = [ [     'GroomFather',    'FATHER',     'Groom'       ],
              [     'GroomMother',    'MOTHER',     'Groom'       ],
              [     'BrideFather',    'FATHER',     'Bride'       ],
              [     'BrideMother',    'MOTHER',     'Bride'       ] ]




def process_record(record):
    persons = []
    relationsDescription = []
    event = {}
    header = {}
    case = 0

    ############ CASE CONTROL
    if 'RelationEP'in record:
        if type(record['RelationEP']) is list:
            pass
        else:
            return False
    else:
        return False

    for person in record['Person']:
        persons.append( flatten_dict(person) )

    for relation in record['RelationEP']:
        relationsDescription.append(relation['RelationType'])

    ############ CASE CONTROL
    if len(relationsDescription) == 6:
        if relationsDescription.sort() == case_case1:
            case = 1

        else:
            return False
    else:
        return False

    if case == 1:
        for i in xrange(0,len(relationsDescription)):
            relationsDescription[i] = translations_case1[relationsDescription[i]]
    else:
        return False

    event = flatten_dict(record['Event'])

    # Now we create the nodes
    nodes = []
    for person in persons:
        nodes.append( neo4j.Node.cast(person) )

    # Now we create the relationships
    relationships = []
    if case == 1:
        for relation in relations_case1:
            relationships.append(   neo4j.Relationship( nodes[relationsDescription.index(relation[0])],
                                    relation[1],
                                    nodes[relationsDescription.index(relation[2])], db_4_marriage_header_id=record['header']['identifier']) )
        marriage_relationship = neo4j.Relationship(nodes[relationsDescription.index('Groom')],
                                                        'MARRIED',
                                                        nodes[relationsDescription.index('Bride')],
                                                        db_4_marriage_header_id=record['header']['identifier'] )
        for key in event:
            marriage_relationship.properties[key] = event[key]

        for node in nodes:
            node.labels.add('Person')
            graph.create(node)

        for relationship in relationships:
            graph.create(relationship)
        graph.create(marriage_relationship)

    else:
        return False

    counter[0] = counter[0]+1

    return True








neo4j.authenticate("localhost:7474", "######", "######") # add credentials
graph = neo4j.Graph("http://localhost:7474/db/data/")




client = MongoClient('mongodb://localhost:27017')
db = client['bhic-databases2']
collection_1 = db['4-civil-status-marriage-acts']
collection_2 = db['4-civil-status-marriage-acts-IMPORTED_TO_NEO4J']

skiped = 1
counter = [1]
records = collection_1.find({})
for record in records:
    if process_record(record) == True:
        collection_2.insert_one(record)
        collection_1.delete_one({'_id': ObjectId(record['_id'])})
    else:
        skiped += 1

    print 'IMPORTED', counter[0], '  SKIPED: ', skiped