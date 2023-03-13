import csv
import json
import os
import pyodbc
import glob
import pandas as pd



# DB server address
server = 'mysqlserver300.database.windows.net'
# connecting DB name
database = 'mySampleDatabase2'
# user name
username = 'cs15'
# user password
password = 'Computer@2020'

# MSSQL connection
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
cursor = cnxn.cursor()

# Directory to expert articles JSON
document_expert_json_dir = "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/2020-10-22-research article"
# Directory to consumer articles JSON
document_consumer_json_dir = "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/epic_qa_consumer_2020_11-02"

# Directory to expert questions prelim
prelim_question_expert_json_dir = "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/others/preliminary/expert_questions_prelim.json"
# Directory to consumer questions prelim
prelim_question_consumer_json_dir = "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/others/preliminary/consumer_questions_prelim.json"

# Directory to judgements file
judgements_json_dir = "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/others/corrected_judgments.json"


def get_data_from_filedir(file):
    # Opening JSON file
    f = open(file, "r")

    # returns JSON object as 
    # a dictionary
    data = json.load(f)
    
    # Closing file
    f.close()

    return data


def add_document_consumer():
    document_consumer_directories = [os.path.join(document_consumer_json_dir, folder) for folder in os.listdir(document_consumer_json_dir)]
    data_list = []
    for dir in document_consumer_directories:
        for file in os.listdir(dir):
            if file.endswith(".json"):
                consumer_data = get_data_from_filedir(os.path.join(dir, file)) # Only one dictionary data
                doc_id = consumer_data["document_id"]
                title = consumer_data["metadata"]["title"] if consumer_data["metadata"]["title"] != None else ""
                authors = ""
                url = consumer_data["metadata"]["url"]

                data = (doc_id, title, authors, url)
                data_list.append(data)

    query = ("INSERT INTO DOCUMENT VALUES (?, ?, ?, ?)")
    cursor.fast_executemany = True
    cursor.executemany(query, data_list)
    cnxn.commit()


def add_document_expert():
    data_list = []
    for file in os.listdir(document_expert_json_dir):
        if file.endswith(".json"):
            expert_data = get_data_from_filedir(os.path.join(document_expert_json_dir, file)) # Only one dictionary data
            doc_id = expert_data["document_id"]
            title = expert_data["metadata"]["title"]
            authors = expert_data["metadata"]["authors"]
            urls = ';'.join(expert_data["metadata"]["urls"])

            data = (doc_id, title, authors, urls)
            data_list.append(data)

    query = ("INSERT INTO DOCUMENT VALUES (?, ?, ?, ?)")
    cursor.fast_executemany = True
    cursor.executemany(query, data_list)
    cnxn.commit()


def add_context_consumer():
    document_consumer_directories = [os.path.join(document_consumer_json_dir, folder) for folder in os.listdir(document_consumer_json_dir)]
    data_list = []
    for dir in document_consumer_directories:
        for file in os.listdir(dir):
            if file.endswith(".json"):
                consumer_data = get_data_from_filedir(os.path.join(dir, file)) # Only one dictionary data
                context_data = consumer_data["contexts"]
                for data in context_data:
                    cont_id = data["context_id"]
                    cont_section = data["section"] if (data["section"] != None and len(data["section"]) < 4000) else ""
                    cont_text = data["text"]
                doc_id = consumer_data["document_id"]

                data = (cont_id, cont_section, cont_text, doc_id)
                data_list.append(data)

    print("Inserting Start !!!")
    query = ("INSERT INTO CONTEXT VALUES (?, ?, ?, ?)")
    cursor.fast_executemany = True
    cursor.executemany(query, data_list)
    cnxn.commit()


def add_context_expert():
    data_list = []
    for file in os.listdir(document_expert_json_dir):
        if file.endswith(".json"):
            expert_data = get_data_from_filedir(os.path.join(document_expert_json_dir, file)) # Only one dictionary data
            context_data = expert_data["contexts"]
            for data in context_data:
                cont_id = data["context_id"]
                cont_section = data["section"]
                cont_text = data["text"]
            doc_id = expert_data["document_id"]

            data = (cont_id, cont_section, cont_text, doc_id)
            data_list.append(data)

    print("Inserting Start !!!")
    query = ("INSERT INTO CONTEXT VALUES (?, ?, ?, ?)")
    cursor.fast_executemany = True
    cursor.executemany(query, data_list)
    cnxn.commit()


# def add_sentence_expert():
#     data_list = []
#     for file in os.listdir(document_expert_json_dir):
#         if file.endswith(".json"):
#             expert_data = get_data_from_filedir(os.path.join(document_expert_json_dir, file)) # Only one dictionary data
#             context_data = expert_data["contexts"]
#             for c_data in context_data:
#                 cont_id = c_data["context_id"]
#                 for sent_data in c_data["sentences"]:
#                     q_id = ""
#                     sent_id = sent_data["sentence_id"]
#                     sent_start = sent_data["start"]
#                     sent_end = sent_data["end"]
#
#                     # judgements_data = get_data_from_filedir(judgements_json_dir)
#                     # for question in judgements_data:
#                     #     for sentence in question["annotations"]:
#                     #         if sent_id == sentence["sentence_id"]:
#                     #             q_id = question["question_id"]
#
#                     data = (sent_id, cont_id, sent_start, sent_end, q_id)
#                     data_list.append(data)
#
#     query = ("INSERT INTO SENTENCE VALUES (?, ?, ?, ?, ?)")
#     cursor.fast_executemany = True
#     cursor.executemany(query, data_list)
#     cnxn.commit()


def add_sentence_nugget_only():

    query = "SELECT * FROM NUGGET_SENTENCE"
    cursor.execute(query)

    nugget_sentence_rows = cursor.fetchall()

    expert_id_length = 8
    cs_chqascience_id_length = 40
    cs_news_id_length = 36

    sentence_list = []
    context_list = []
    document_list = []

    for row in nugget_sentence_rows:
        doc_id = ""
        doc_id_split = str(row.sent_id).split('-')
        if len(doc_id_split) > 4:
            doc_id = '-'.join(doc_id_split[0:5])
        else:
            doc_id = doc_id_split[0]

        if len(doc_id) == expert_id_length:
            try:
                all_data = get_data_from_filedir("C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/2020-10-22-research article/" + doc_id + ".json")
            except IOError:
                all_data = get_data_from_filedir("C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/expert_prelim_doc/" + doc_id + ".json")

                doc_id = all_data["document_id"]
                title = all_data["metadata"]["title"]
                authors = all_data["metadata"]["authors"]
                urls = ';'.join(all_data["metadata"]["urls"])

                data = (doc_id, title, authors, urls)
                if data not in document_list:
                    document_list.append(data)

                context_data = all_data["contexts"]
                for data in context_data:
                    cont_id = data["context_id"]
                    cont_section = data["section"] if (data["section"] != None and len(data["section"]) < 4000) else " "
                    cont_text = data["text"]
                doc_id = all_data["document_id"]

                data = (cont_id, cont_section, cont_text, doc_id)
                if data not in context_list:
                    context_list.append(data)

        elif len(doc_id) == cs_news_id_length:
            all_data = get_data_from_filedir("C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/epic_qa_consumer_2020_11-02/ccns-trec/" + doc_id + ".json")
        elif len(doc_id) == cs_chqascience_id_length:
            try:
                all_data = get_data_from_filedir(
                    "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/epic_qa_consumer_2020_11-02/ask_science-2020-10-29/" + doc_id + ".json")
            except IOError:
                try:
                    all_data = get_data_from_filedir(
                        "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/epic_qa_consumer_2020_11-02/chqa-2020-10-09/" + doc_id + ".json")
                except IOError:
                    all_data = get_data_from_filedir(
                        "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/consumer_prelim_doc/" + doc_id + ".json")

                    doc_id = all_data["document_id"]
                    title = all_data["metadata"]["title"] if all_data["metadata"]["title"] != None else ""
                    authors = ""
                    url = all_data["metadata"]["url"]

                    data = (doc_id, title, authors, url)
                    if data not in document_list:
                        document_list.append(data)

                    context_data = all_data["contexts"]
                    for data in context_data:
                        cont_id = data["context_id"]
                        cont_section = data["section"] if (data["section"] != None and len(data["section"]) < 4000) else " "
                        cont_text = data["text"]
                    doc_id = all_data["document_id"]

                    data = (cont_id, cont_section, cont_text, doc_id)
                    if data not in context_list:
                        context_list.append(data)

        context_data = all_data["contexts"]
        for c_data in context_data:
            cont_id = c_data["context_id"]
            for sent_data in c_data["sentences"]:
                q_id = ""
                sent_id = sent_data["sentence_id"]
                sent_start = sent_data["start"]
                sent_end = sent_data["end"]

                data = (sent_id, cont_id, sent_start, sent_end, q_id)
                if (row.sent_id == sent_id and data not in sentence_list):
                    sentence_list.append(data)

    print("Inserting Start (DOCUMENT) !!!")
    query = ("INSERT INTO DOCUMENT VALUES (?, ?, ?, ?)")
    cursor.fast_executemany = True
    cursor.executemany(query, document_list)

    print("Inserting Start (CONTEXT) !!!")
    query = ("INSERT INTO CONTEXT VALUES (?, ?, ?, ?)")
    cursor.fast_executemany = True
    cursor.executemany(query, context_list)

    print("Inserting Start (SENTENCE) !!!", len(sentence_list))
    query = ("INSERT INTO SENTENCE VALUES (?, ?, ?, ?, ?)")
    cursor.fast_executemany = True
    cursor.executemany(query, sentence_list)

    cnxn.commit()


def add_prelim_question_expert():
    q_data = get_data_from_filedir(prelim_question_expert_json_dir)

    data_list = []
    for q in q_data:
        q_id = q["question_id"]
        q_text = q["question"]
        q_query = q["query"]
        q_background = q["background"]

        data = (q_id, q_text, q_query, q_background)
        data_list.append(data)

    query = ("INSERT INTO QUESTION VALUES (?, ?, ?, 'EQ', ?)")
    cursor.fast_executemany = True
    cursor.executemany(query, data_list)
    cnxn.commit()
    

def add_prelim_question_consumer():
    q_data = get_data_from_filedir(prelim_question_consumer_json_dir)

    data_list = []
    for q in q_data:
        q_id = q["question_id"]
        q_text = q["question"]
        q_query = q["query"]
        q_background = q["background"]

        data = (q_id, q_text, q_query, q_background)
        data_list.append(data)

    query = ("INSERT INTO QUESTION VALUES (?, ?, ?, 'CQ', ?)")
    cursor.fast_executemany = True
    cursor.executemany(query, data_list)
    cnxn.commit()


def add_nugget():
    judgements_data = get_data_from_filedir(judgements_json_dir)
    for question in judgements_data:
        q_id = question["question_id"]
        for nugget in question["nuggets"]:
            nggt_id = nugget["nugget_id"]
            nggt_text = nugget["nugget"]
            for sentence in question["annotations"]:
                if nggt_id in sentence["nugget_ids"]:
                    sent_id = sentence["sentence_id"]
                    query = "INSERT INTO NUGGET_SENTENCE (nggt_id, sent_id) VALUES (?, ?)"
                    data = (nggt_id, sent_id)
                    cursor.execute(query, data)

            query = ("INSERT INTO NUGGET VALUES (?, ?, ?)")
            data = (nggt_id, nggt_text, q_id)
            cursor.execute(query, data)

    cnxn.commit()



# add_document_expert()
# add_context_expert()
# add_sentence_expert()
# add_prelim_question_expert()

# add_document_consumer()
# add_context_consumer()
# add_sentence_consumer()
# add_prelim_question_consumer()

# add_sentence_nugget_only()

# add_nugget()



# Closing connection
cursor.close()
cnxn.close()