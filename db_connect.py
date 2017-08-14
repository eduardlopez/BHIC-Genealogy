# -*- coding: utf-8 -*-
from config import *
from pymongo import MongoClient
from bson.objectid import ObjectId
import pprint

pp = pprint.PrettyPrinter(indent=2)

# Connect to the database
def mongo_connect():
    client = MongoClient('mongodb://' + username() + ":" + password() + "@"  + ip() + ":" + str(port()) + '/')
    bhic = client['bhic-databases']

    # Load collections
    births = bhic['civil-status-births-certificates']
    baptisms = bhic['dtb-baptisms-certificates']
    marriage_acts = bhic['civil-status-marriage-acts']
    marriage_actions = bhic['dtb-marriage-actions']
    deaths = bhic['civil-status-deaths']
    death_actions = bhic['dtb-death-actions']
    succession = bhic['memories-of-succession']
    pop_registers = bhic['genealogical-population-registers']
    military = bhic['military-register']
    prison = bhic['prision-register']
    people = bhic['people']
    people_debug = bhic['people_debug']
    errors = bhic['errors']

    # Dict containing all collections of the original data source (as imported)
    source_collections = {'births':births, 'baptisms':baptisms,
    'marriage_acts':marriage_acts, 'marriage_actions':marriage_actions,
    'deaths':deaths, 'death_actions':death_actions, 'succession':succession,
    'pop_registers':pop_registers, 'military':military, 'prison':prison}

    return {'client': client, 'bhic': bhic,
            'source_collections': source_collections,
            'people':people, 'people_debug': people_debug, 'errors': errors}

# Query all collections in the "source_collections" dictionary
def query_all_source_collections(query, nr_results, verbose):
    mc = mongo_connect()

    for collection in mc['source_collections']:
        if verbose == True:
            for n, document in enumerate(mc['source_collections'][collection].find(query)):
                pp.pprint(document)

                if n>nr_results:
                    break

        print("# records in", collection, mc['source_collections'][collection].find(query).count())


if __name__ == "__main__":
    mc = mongo_connect()

    # test by counting # records in each collection
    for collection in mc['source_collections']:
        # print(type(collection))
        print("# records in", collection, mc['source_collections'][collection].count())

    print("# records in people", mc['people'].count())
