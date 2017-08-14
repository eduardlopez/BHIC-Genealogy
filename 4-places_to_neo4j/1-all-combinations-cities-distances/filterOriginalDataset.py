__author__ ="eduardlopez"

import csv
import re

# List of cities, towns and villages in North Brabant
# https://en.wikipedia.org/wiki/List_of_cities,_towns_and_villages_in_North_Brabant
# THE COLUMN "Name" IS THE ONE THAT WE WANT, INCLUDES ALL UNIQUE NAMES OF ALL CITIES, TOWNS AND VILLAGES
# IN THE PROVINCE OF NORTH BRABANT, IN THE NETHERLANDS

re1='.*?'	# Non-greedy match on filler
re2='([+-]?\\d*\\.\\d+)(?![-+0-9\\.])'	# Float 1
re3='.*?'	# Non-greedy match on filler
re4='([+-]?\\d*\\.\\d+)(?![-+0-9\\.])'	# Float 2
rg = re.compile(re1+re2+re3+re4,re.IGNORECASE|re.DOTALL)


newHeader = ["Place","Latitude","Longitude"]

csvOriginalReader = csv.reader(open("1-originalDataset.csv", encoding="utf-8"))
csvWriter = csv.writer(open("2-places-latitude-longitude.csv",'w', encoding="utf-8",newline=''))
csvWriter.writerow(newHeader)

finalRow_0 = []
finalRow = []
iter = 0
for row in csvOriginalReader:
    print(row)
    m = rg.search(row[2])
    if m:
        float1 = m.group(1)
        float2 = m.group(2)
        print("(" + float1 + ")" + "(" + float2 + ")")

        finalRow = [row[0].replace("'", ""), float1, float2] # We delete al the ' symbol

        if iter > 0:
            if not finalRow_0[0] == finalRow[0]:
                csvWriter.writerow(finalRow)
                print(finalRow, "\n")
        else:
            csvWriter.writerow(finalRow)
            print(finalRow, "\n")

        finalRow_0 = finalRow
        iter += 1