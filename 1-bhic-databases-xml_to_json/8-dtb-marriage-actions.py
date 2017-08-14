__author__ ="eduardlopez"
#import gc
from collections import defaultdict
import json
import sys
import xml.etree.ElementTree as ET

from pymongo import MongoClient


nameCollection = "8-dtb-marriage-actions"
filename = nameCollection+".xml"
intermediate = nameCollection+"_INTERMEDIATE.txt"

client = MongoClient('localhost', 27017)
db = client.local
collection = db[nameCollection]





#gc.enable()

def parse_xml(file_name):
    events = ("start", "end")
    context = ET.iterparse(file_name, events=events)
    pt(context)



no_tags = ["OAI-PMH", "request", "responseDate", "ListRecords", "metadata", "A2A"]


#count = 0

def pt(context, cur_elem=None):
    #global count
    items = defaultdict(list)

    if cur_elem is not None:
        items.update(cur_elem.attrib)

    text = ""

    for action, elem in context:
        # print("{0:>6} : {1:20} {2:20} '{3}'".format(action, elem.tag, elem.attrib, str(elem.text).strip()))

        tag = elem.tag.split('{')[1].split('}')[1]
        """
        if tag == "record":
            if count > 3000:
                gc.collect()
                print "collecting"
                count = 0
            else:
                count = count + 1
        """
        if tag not in no_tags:
            if action == "start":
                tag = elem.tag.split('{')[1].split('}')[1]

                t = pt(context, elem)
                if t is not None:
                    items[tag].append(t)
            elif action == "end":
                text = elem.text.strip() if elem.text else ""
                if tag == "record":
                    record = {k: v[0] if len(v) == 1 else v for k, v in items.items()}
                    collection.insert_one(record).inserted_id
                    return None
                break

    if len(items) == 0:
        return text
    #print
    #gc.garbage
    return {k: v[0] if len(v) == 1 else v for k, v in items.items()}



nRecords = 0
nMAXRecords = 2000
lines = ''



header = open("header.xml")
header = header.read()
footer = open("footer.xml")
footer = footer.read()

no_tags2 = ["?xml", "<OAI-PMH", "<responseDate>", "<request verb=", " <ListRecords"]

recordInLine = False

with open(filename) as f:


    for line in f:
        PASSline = False
        recordInLine = False

        if "<record" in line:
            a = line.split("<record")
            a[1] = '<record' + a[1]
            recordInLine = True


        if recordInLine == False:

            for n_t in no_tags2 :
                if n_t in line:
                    PASSline = True



            if PASSline is not True:
                if "</record" in line:
                    nRecords += 1
                    if nRecords >= nMAXRecords:
                        lines = lines + line
                        lines = header + lines + footer
                        intermediate_file = open(intermediate, "w")
                        intermediate_file.write(lines)
                        intermediate_file.close()
                        lines = ''
                        parse_xml(intermediate)
                        nRecords = 0
                    else:
                        lines = lines + line

                else:
                    lines = lines + line

        else:



            for alem in a:
                line = alem

                if "<ListRecords" in alem:
                    continue


                if PASSline is not True:
                    if "</record" in line:
                        nRecords += 1
                        if nRecords >= nMAXRecords:
                            lines = lines + line
                            if "</record></ListRecords>" in line:
                                lines = header + lines
                            else:
                                lines = header + lines + footer
                            intermediate_file = open(intermediate, "w")
                            intermediate_file.write(lines)
                            intermediate_file.close()
                            lines = ''
                            parse_xml(intermediate)
                            nRecords = 0
                        else:
                            lines = lines + line

                    else:
                        lines = lines + line



if nRecords > 0:
    lines = lines
    lines = header + lines
    intermediate_file = open(intermediate, "w")
    intermediate_file.write(lines)
    intermediate_file.close()
    lines = ''
    parse_xml(intermediate)
    nRecords = 0

#json_data = parse_xml(filename)



