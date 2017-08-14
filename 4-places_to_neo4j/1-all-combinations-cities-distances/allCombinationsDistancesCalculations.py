__author__ ="eduardlopez"

import csv
import geopy.distance
from itertools import combinations

elements = ['A','B','C']
def getCombinations(elementsList):
    return [list(comb) for comb in combinations(elementsList, 2)]

print('Example: elements = ', elements)
print("Unique Combinations = ", getCombinations(elements),"\n")

csvOriginalReaderDict = csv.DictReader(open("2-places-latitude-longitude.csv", encoding="utf-8"))
csvWriter = csv.writer(open("3-place-place-distanceKM.csv",'w', encoding="utf-8",newline=''))

newHeader = ["Place1","Place2","DistanceKM"]
csvWriter.writerow(newHeader)

rowsDictKeys = []
dictKeys = {}
for row in csvOriginalReaderDict:
    rowsDictKeys.append(row['Place'])
    dict = {}
    dict['Longitude'] = row['Longitude']
    dict['Latitude'] = row['Latitude']
    dictKeys[row['Place']] = dict
    # from this:    'Place': 'Aalst', 'Longitude': '5.47778', 'Latitude': '51.39583'}
    # to this :     {'Aalst': {'Longitude': '5.47778', 'Latitude': '51.39583'}}


allRows = []
for combination in getCombinations(rowsDictKeys):
    coords_1 = ( dictKeys[combination[0]]['Latitude'], dictKeys[combination[0]]['Longitude'] )
    coords_2 = ( dictKeys[combination[1]]['Latitude'], dictKeys[combination[1]]['Longitude'] )
    allRows.append( combination + [round(geopy.distance.vincenty(coords_1, coords_2).km, 1)] )
    print(allRows[-1])


csvWriter.writerows(allRows)
print(allRows)
