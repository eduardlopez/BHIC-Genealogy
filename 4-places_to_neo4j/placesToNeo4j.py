__author__ ="eduardlopez"
import csv
from pymongo import *
import py2neo as neo4j

session = neo4j.Graph(password="admin")

csvOriginalReaderDict = csv.DictReader(open("3-place-place-distanceKM.csv"))
iteration = 0
dictKeys = {}
for row in csvOriginalReaderDict:
    query_part1 = "MERGE ( p1:Place { Place:'" + row['Place1'] + "' } )"
    query_part2 = "MERGE ( p2:Place { Place:'" + row['Place2'] + "' } )"
    query_part3 = "MERGE (p1)-[r1:Distance{distance:toFloat("+ row['DistanceKM'] + ")}]->(p2)"

    final_query = query_part1 + query_part2 + query_part3
    #print final_query
    session.run(final_query)
    if iteration%100 == 0:
        print iteration
    iteration += 1