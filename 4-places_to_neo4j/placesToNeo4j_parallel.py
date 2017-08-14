__author__ ="eduardlopez"
import csv
from pymongo import *
import py2neo as neo4j
import multiprocessing

session = neo4j.Graph(password="admin")

csvOriginalReaderDict = csv.DictReader(open("3-place-place-distanceKM.csv"))
csvOriginalReaderDict = list(csvOriginalReaderDict)

dictKeys = {}


nrows = 585904

def worker2(nthreads, threadId):
	sizeWork = int(nrows/nthreads)
	iteration = sizeWork*threadId


	for i in range(sizeWork*threadId, sizeWork*threadId+sizeWork):
		query_part1 = "MERGE ( p1:Place { Place:'" + csvOriginalReaderDict[i]['Place1'] + "' } )"
		query_part2 = "MERGE ( p2:Place { Place:'" + csvOriginalReaderDict[i]['Place2'] + "' } )"
		query_part3 = "MERGE (p1)-[r1:Distance{distance:toFloat("+ csvOriginalReaderDict[i]['DistanceKM'] + ")}]->(p2)"

		final_query = query_part1 + query_part2 + query_part3
		#print final_query
		#print final_query
		session.run(final_query)
		if iteration%100 == 0:
			print iteration
		iteration += 1




nthreads = 8*8


if __name__ == '__main__':

	jobs = []

	for threadId in range(0, nthreads):  # [0,1]
		p = multiprocessing.Process(target=worker2, args=(nthreads, threadId) )
		p.start()
		jobs.append(p)


	for job in jobs:
		job.join()




