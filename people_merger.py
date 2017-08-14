from db_connect import mongo_connect
from nltk.metrics import *
import queue
import threading
import random
import time
import json
import itertools
import pprint

debugging = False
dry_run = False
read_table = 'people_debug'
write_table = 'people_debug'
maxAllowedDistanceLevenshtein = 2

locked_documents = []
pp = pprint.PrettyPrinter(indent=2)
exitflag = 0
count = 0
mc = mongo_connect()


class myThread (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        print("Starting " + self.name)
        process_data(self.name, self.q)
        print("Exiting " + self.name)


def process_data(thread_name, q):
    global count
    global n_data
    while not exitflag:
        queueLock.acquire()
        if not workQueue.empty():
            q_item = q.get()
            queueLock.release()
            try:
                print(thread_name, count, '/', n_data)
                merge_person(q, thread_name, q_item[0], q_item[1])
                count += 1
            except:
                queueLock.acquire()
                q.put(q_item)
                queueLock.release()
                time.sleep(1)
                print('error')
        else:
            queueLock.release()
        time.sleep(0.1)


# Check if the roles are married parents
def is_married_parent(role1, role2):
    return True if role1 in ['HasChildWith', 'MarriedTo'] and role2 in ['HasChildWith', 'MarriedTo'] else False


# Calculate if a name is roughly the same using Levenshtein
def name_validation(string1, string2):
    if None in [string1, string2]:
        return False
    else:
        return True if edit_distance(string1, string2) <= maxAllowedDistanceLevenshtein else False


# TODO Lock documents before writing
def relation_checker(relatives):
    relations_with_multiple_pids = []
    # Do a cartesian product where relation[0] == relation[1] and pid[0[!=pid[1]
    for i in itertools.product(relatives, relatives):
        if (i[0]['Relation'] == i[1]['Relation'] or is_married_parent(i[0]['Relation'], i[1]['Relation'])) and i[0]['pid'] != i[1]['pid']:
            pid_pair = [i[0]['pid'], i[1]['pid']]
            pid_pair.sort()
            pid_pair = tuple(pid_pair)
            relations_with_multiple_pids.append((pid_pair, i[0]['Relation']))

    # remove duplicates
    relations_with_multiple_pids = list(set(relations_with_multiple_pids))

    relatives_with_multiple_pids = []
    for pids_relation in relations_with_multiple_pids:
        person1 = mc[read_table].find_one({'_id': pids_relation[0][0]})
        person2 = mc[read_table].find_one({'_id': pids_relation[0][1]})

        if person1 is not None and person2 is not None:
            # TODO Minimum edit distance of two strings (lastName and first name)
            # TODO Pick the right name
            if pids_relation[1] in ['FatherOf', 'MotherOf']:
                relatives_with_multiple_pids.append(pids_relation)
            else:
                match_first_name = name_validation(person1.get('PersonNameLastName'), person2.get('PersonNameLastName'))
                match_last_name = name_validation(person1.get('PersonNameFirstName'),
                                                  person2.get('PersonNameFirstName'))

                if match_first_name and match_last_name and \
                                None not in [person1.get('PersonNameLastName'), person2.get('PersonNameLastName'),
                                             person1.get('PersonNameFirstName'), person2.get('PersonNameFirstName')]:
                    relatives_with_multiple_pids.append(pids_relation)
                    if debugging:
                        print('match')
                        print(pids_relation[0][0], person1.get('PersonNameLastName'),
                              person1.get('PersonNameFirstName'))
                        print(pids_relation[0][1], person2.get('PersonNameLastName'),
                              person2.get('PersonNameFirstName'))
                else:
                    if debugging:
                        print('No match:', pids_relation[0][0], person1.get('PersonNameLastName'),
                              person1.get('PersonNameFirstName'))
                        print('No match:', pids_relation[0][1], person2.get('PersonNameLastName'),
                              person2.get('PersonNameFirstName'))
    return relatives_with_multiple_pids


def remove_links_to_old_pid(blk, pid1, pid2):
    for personWithDangingLinks in mc[read_table].find({'relatives.pid': pid2}):
        for relative in personWithDangingLinks['relatives']:
            if relative['pid'] == pid2:
                relative['pid'] = pid1

                # print(personWithDangingLinks['relatives'])

        # Remove duplicates
        # TODO Check if this works correctly
        personWithDangingLinks['relatives'] = [dict(tpl) for tpl in
                                               set([tuple(dct.items()) for dct in personWithDangingLinks['relatives']])]

        blk.find({'_id': personWithDangingLinks['_id']}).update(
            {'$set': {'relatives': personWithDangingLinks['relatives']}})


def merge_person(q, t_name, pid1, pid2):
    bulk = mc[write_table].initialize_ordered_bulk_op()

    # Get people from the database
    print(t_name, 'Merging:', pid1, pid2)
    person1 = mc[read_table].find_one({'_id': pid1})
    person2 = mc[read_table].find_one({'_id': pid2})

    # If person1 or person2 are empty return
    if person1 is None or person2 is None:
        return

    # Check where the information is contained person1 ->left, person2 -> right
    both = []
    left = []
    right = []

    for key in person1.keys():
        if key in person2.keys():
            both.append(key)
        else:
            left.append(key)

    for key in person2.keys():
        if key not in person1.keys():
            right.append(key)

    # TODO Keep the most complete record
    person_merged = {}
    for key in both:
        if key == 'Sources':
            person_merged[key] = []
            for source in person1[key]:
                person_merged[key].append(source)
            for source in person2[key]:
                person_merged[key].append(source)
        elif key == 'Gender':
            if person1[key] == 'Onbekend':
                person_merged[key] = person2[key]
            else:
                person_merged[key] = person1[key]
        elif key == 'relatives':
            person_merged[key] = []
            # TODO Check for double links after merge

            for relative in person1[key]:
                person_merged[key].append(relative)

            for relative in person2[key]:
                if relative not in person_merged[key]:
                    person_merged[key].append(relative)
        elif key == 'pid':
            person_merged[key] = person1[key]
        else:
            # if debugging:
            #     print(person1[key], person2[key])
            person_merged[key] = person1[key]

    for key in left:
        person_merged[key] = person1[key]

    for key in right:
        person_merged[key] = person2[key]

    # # Check for dangling links!!!
    remove_links_to_old_pid(bulk, pid1, pid2)

    bulk.find({'_id': pid1}).replace_one(person_merged)
    bulk.find({'_id': pid2}).remove()

    if not dry_run:
        bulk.execute()

    # Find if there are relations with multiple pid's and check if they are the same person
    relatives_with_multiple_pids = relation_checker(person_merged['relatives'])

    # TODO Remove duplicates

    # Recurse merge_person
    # print(relatives_with_multiple_pids)
    for relative in relatives_with_multiple_pids:
        try:
            merge_person(q, t_name, relative[0][0], relative[0][1])
        except:
            queueLock.acquire()
            q.put((relative[0][0], relative[0][1]))
            queueLock.release()
            time.sleep(1)
            print('error')


def build_queue(dta):
    n_dta = 0
    work = []
    for pid_group in dta:
        while len(pid_group) > 1:
            work.append((pid_group[0], pid_group[1]))
            del pid_group[1]

    # Scramble work
    random.shuffle(work)

    # build actual queue
    queueLock.acquire()
    for pair in work:
        n_dta += 1
        workQueue.put(pair)
    queueLock.release()

    return n_dta


if __name__ == "__main__":
    threadList = ['Thread-' + str(i) for i in range(0, 30)]

    queueLock = threading.Lock()
    workQueue = queue.Queue()
    threads = []
    threadID = 1

    # Create new threads
    for tName in threadList:
        thread = myThread(threadID, tName, workQueue)
        thread.start()
        threads.append(thread)
        threadID += 1

    with open('matches.json') as data_file:
        data = json.load(data_file)



    # Build a scrambled queue
    n_data = build_queue(data)

    # Wait for queue to empty
    while not workQueue.empty():
        pass

    # Notify threads it's time to exit
    exitflag = 1
