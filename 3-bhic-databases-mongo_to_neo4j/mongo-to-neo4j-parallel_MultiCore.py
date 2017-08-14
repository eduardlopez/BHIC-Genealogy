__author__ ="eduardlopez"
from pymongo import *
import py2neo as neo4j
#from neo4j.v1 import GraphDatabase
import time
import multiprocessing
import pprint as pp
import re

def flatten_dict(d):
    def expand(key, value):
        if isinstance(value, dict):
            return [ (key + '_' + k, v) for k, v in flatten_dict(value).items() ]
        else:
            return [ (key, value) ]
    items = [ item for k, v in d.items() for item in expand(k, v) ]
    return dict(items)



def create_node(properties, avoid_keys, session):
    avoid_keys.append('pid'); avoid_keys.append('_id')
    merge_part = "MERGE ( p:Person { pid:'"+properties['pid']+"' } )"
    #on_create_set_part = ' ON CREATE SET '+'p.id='+properties['id']+" ,"
    on_create_set_part = ' ON CREATE SET '
    on_match_set_part = ' ON MATCH SET '
    for key, value in properties.items():
        if key not in avoid_keys:
            #print key, value
            on_create_set_part  += 'p.' + key + '=' + "'" + value.replace("'", "") + "'" + ','
            on_match_set_part   += 'p.' + key + '=' + "'" + value.replace("'", "") + "'" + ','

    on_create_set_part = on_create_set_part[:-1] # The last comma it's removed
    on_match_set_part = on_match_set_part[:-1] # The last comma it's removed

    final_query = merge_part + on_create_set_part + on_match_set_part

    session.run(final_query)

    return True



def create_relations(pid, relative, session):
    match_part1 = "MERGE ( p1:Person { pid:'" + pid + "' } )"
    match_part2 = "MERGE ( p2:Person { pid:'" + relative['pid'] + "' } )"

    relationship_properties = ''
    for key, value in relative.items():
        if key not in ['pid','Relation','temporaryRelation']:
            #print relative, value
            relationship_properties += key + ":'" + value.replace("'", "") + "',"

    relationship_properties = relationship_properties[:-1]

    if relationship_properties == '':
        merge_part3  = "MERGE (p2)<-[r1:"+relative['Relation']+"]-(p1)"
    else:
        merge_part3 = "MERGE (p2)<-[r1:" + relative['Relation'] + "{" + relationship_properties + "} ]-(p1)"

    final_query = match_part1 + match_part2 + merge_part3

    session.run(final_query)

    return True



def create_source(CollectionName, HeaderID, pid, session):
    match_part1 = "MERGE ( p1:Person { pid:'" + pid + "' } )"
    match_part2 = "MERGE ( s1:Source { CollectionName:'" + CollectionName + "'" + ", HeaderID:'" + HeaderID + "'} )"
    merge_part3 = "MERGE (s1)<-[r1:" + 'Source' + "]-(p1)"

    final_query = match_part1 + match_part2 + merge_part3

    session.run(final_query)

    return True


def create_birthPlace(birthPlace, pid, session):
    match_part1 = "MERGE ( p1:Person { pid:" + "'" + pid + "'" + "} )"
    match_part2 = "MERGE ( s2:Place { Place:" + "'" + birthPlace.replace("'", "") + "'" + "} )"
    merge_part3 = "MERGE (s2)<-[r1:" + 'BirthPlace' + "]-(p1)"

    final_query = match_part1 + match_part2 + merge_part3

    session.run(final_query)

    return True



def removekey(d, key):
    r = dict(d)
    del r[key]
    return r

# GENERAL STRUCTURE:
# {
# 	- id = 123
# 	- NameFirstName = "a"
# 	- NameLastName = "b"
# 	- BirthDateDay = 1
# 	- BirthDataMonth = 1
# 	- BirthDateYear = 1111
#	- BirthPlace = "Eindhoven"
# 	Relatives
# 		[0]
# 			- id
# 			- Relation
# 		[0]
# 			- id
# 			- Relation
# 	Source
# 		[0]
# 			- CollectionName
# 			- HeaderID
# 			- pid
# 		[1]
# 			- CollectionName
# 			- HeaderID
# 			- pid
# }
def process_record(record, session):
    # type(record) = dict

    # First we create the principal node Person
    avoid_keys = ['relatives','Sources','BirthDate','BirthPlace','temporaryRelation']

    record_flatted = record

    if ('relatives' in record):
        record_flatted = removekey(record_flatted, 'relatives')

    if ('Sources' in record):
        record_flatted = removekey(record_flatted, 'Sources')


    record_flatted = flatten_dict(record_flatted)


    if create_node(record_flatted, avoid_keys, session) is not True:
        print("ERROR IN create_node")
        return False


    # Then we set the relations with principal node
    if ('relatives' in record) and (len(record['relatives'])>0):
        for relative in record['relatives']:

            if relative['DateFrom']:
                if relative['DateFrom'].year:
                    if relative['DateFrom'].year != 0:
                        relative['DateFrom_Year'] = str(relative['DateFrom'].year)
                if relative['DateFrom'].month:
                    if relative['DateFrom'].month != 0:
                        relative['DateFrom_Month'] = str(relative['DateFrom'].month)
                if relative['DateFrom'].day:
                    if relative['DateFrom'].day != 0:
                        relative['DateFrom_Day'] = str(relative['DateFrom'].day)
                relative = removekey(relative, 'DateFrom')
            if relative['DateTo']:
                if relative['DateTo'].year:
                    if relative['DateTo'].year != 0:
                        relative['DateTo_Year'] = str(relative['DateTo'].year)
                if relative['DateTo'].month:
                    if relative['DateTo'].month != 0:
                        relative['DateTo_Month'] = str(relative['DateTo'].month)
                if relative['DateTo'].day:
                    if relative['DateTo'].day != 0:
                        relative['DateTo_Day'] = str(relative['DateTo'].day)
                relative = removekey(relative, 'DateTo')


            if relative['temporaryRelation']:
                if relative['temporaryRelation']=='false':
                    if create_relations(record['pid'], flatten_dict(relative), session) is not True:
                        print("ERROR IN create_relations=false")
                        return False
            else:
                if create_relations(record['pid'], flatten_dict(relative), session) is not True:
                    print("ERROR IN create_relations=else")
                    return False


    # Now we create the nodes-relations of the Sources
    if ('Sources' in record) and (len(record['Sources'])>0):
        records = record['Sources']
        for source in records:
            if create_source(source['Collection'], source['SourceHeaderIdentifier'], record['pid'], session) is not True:
                print("ERROR IN sources")
                return False


    # Now we create the nodes-relations for the BirthPlace
    if ('BirthPlace_Place' in record_flatted):
        if create_birthPlace(record_flatted['BirthPlace_Place'], record_flatted['pid'], session) is not True:
            print("ERROR IN BirthPlace_Place")
            return False



    return True



def worker(startJob, endJob, workerid):
    print( startJob, endJob, workerid )


def worker2(nthreads, threadId, stepSize, step,):
    iteration = 0
    
    session = neo4j.Graph(password=######) # add credentials

    client = MongoClient('mongodb://######:######@localhost:27017') # add credentials
    db = client['local']
    # db = client['bhic-databases']
    collection = db['people']
    collection2 = db['people_NOT_INSERTED']

    startJob = 0 ;  endJob = 0
    
    startITERATION = 0; endITERATION = 1
    total_tic = time.time()
    while 1:
        tic = time.time()
        startJob    = (threadId * step) + (nthreads*step*iteration)
        endJob      = (threadId * step) + (nthreads*step*iteration) + step
        records = collection.find(batch_size=step  )[startJob:endJob]

        for record in records:
            try:
                if process_record(record, session) != True:
                    print("\n\n\nERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR")
                    print("ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR")
                    print('StartJob: ', startJob, '  EndJob: ', endJob, '  iteration: ', iteration, '  threadId: ', threadId)
                    print("Document: ")
                    pp.pprint(record)
                    print( "#################### Error by False")
                    try:
                        collection2.insert_one(record)
                    except:
                        print "\n\nDUPLICATE IN MONGO\n\n"
                    print("ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR")
                    print("ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR\n\n\n\n\n\n")
            except:
                print("\n\n\nERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR")
                print("ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR")
                print('StartJob: ', startJob, '  EndJob: ', endJob, '  iteration: ', iteration, '  threadId: ', threadId)
                print("Document: " )
                pp.pprint(record)
                try:
                    collection2.insert_one(record)
                except:
                    print "\n\nDUPLICATE IN MONGO\n\n"
                print("ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR")
                print("ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR\n\n\n\n\n\n")

            endITERATION += 1
            #print 'StartJob: ', startJob, '  EndJob: ', endJob, '  WorkerID: ', workerid, "  RECORD: ", count
        #print( 'StartJob: ', startJob, '  EndJob: ' ,endJob, '  threadId: ' , threadId, "############ END JOB" )


        toc = time.time()
        tt = toc - total_tic
        m = ( ( ( ( (toc-tic)/step)*(13945270-endJob))/60)/nthreads ) - tt/60
        h = m/60
        d = h/24
        print('StartJob: ', startJob, '  EndJob: ', endJob, '  iteration: ', iteration, '  threadId: ', threadId, "############ END JOB", 'TOTAL TIME ITERATION:', toc - tic, "  || TOTAL TIME: ", tt
            ,  "  Restant m, h, d: ", m , h , d )
        startITERATION += 1
        iteration += 1







nthreads = 12*8
stepSize = 10000
step = 5000



if __name__ == '__main__':

    #tic_g = time.clock()
    jobs = []

    for threadId in range(0, nthreads):  # [0,1]
        p = multiprocessing.Process(target=worker2, args=(nthreads, threadId, stepSize, step,) )
        p.start()
        jobs.append(p)


    for job in jobs:
        job.join()

    #toc_g = time.clock()
    #print( 'TOTAL TIME -> stepSize='+str(stepSize)+': ', toc_g - tic_g )

    #print 'IMPORTED', counter[0], '  SKIPED: ', skiped


