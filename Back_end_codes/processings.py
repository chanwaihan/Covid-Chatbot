import os
import json
import pandas as pd
import csv
import pyodbc

# Directory to expert articles JSON
document_expert_json_dir = "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/2020-10-22-research article"
# Directory to consumer articles JSON
document_consumer_json_dir = "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/epic_qa_consumer_2020_11-02"

# Directory for csv
context_expert_csv = "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/context_expert.csv"
context_consumer_csv = "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/context_consumer.csv"


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

def get_data_from_filedir(file):
    # Opening JSON file
    f = open(file, "r", encoding='utf-8')

    # returns JSON object as
    # a dictionary
    data = json.load(f)

    # Closing file
    f.close()

    return data


def change_file_name_news():
    news_dir = "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Project/epic_qa_consumer_2020_11-02/ccns-trec"
    news_id_length = 36
    for file in os.listdir(news_dir):
        if file.endswith(".json"):
            os.rename(news_dir + '/' + file, news_dir + '/' + file[:news_id_length] + ".json")


def convert_expert_context_to_csv():
    columns = ['cont_id', 'cont_section', 'cont_text', 'doc_id']

    with open(context_expert_csv, 'w', encoding='utf-8') as f:
        write = csv.writer(f)
        write.writerow(columns)

        for file in os.listdir(document_expert_json_dir):
            if file.endswith(".json"):
                expert_data = get_data_from_filedir(os.path.join(document_expert_json_dir, file))  # Only one dictionary data
                context_data = expert_data["contexts"]
                for data in context_data:
                    cont_id = data["context_id"]
                    cont_section = data["section"]
                    cont_text = data["text"]
                doc_id = expert_data["document_id"]

                data = [cont_id, cont_section, cont_text, doc_id]
                write.writerow(data)


def convert_consumer_context_to_csv():
    columns = ['cont_id', 'cont_section', 'cont_text', 'doc_id']

    with open(context_consumer_csv, 'w', encoding='utf-8') as f:
        write = csv.writer(f)
        write.writerow(columns)

        document_consumer_directories = [os.path.join(document_consumer_json_dir, folder) for folder in os.listdir(document_consumer_json_dir)]
        for dir in document_consumer_directories:
            for file in os.listdir(dir):
                if file.endswith(".json"):
                    expert_data = get_data_from_filedir(os.path.join(dir, file))  # Only one dictionary data
                    context_data = expert_data["contexts"]
                    for data in context_data:
                        cont_id = data["context_id"]
                        cont_section = data["section"] if (data["section"] != None and len(data["section"]) < 4000) else " "
                        cont_text = data["text"]
                    doc_id = expert_data["document_id"]

                    data = [cont_id, cont_section, cont_text, doc_id]
                    write.writerow(data)


# convert_expert_context_to_csv()
# convert_consumer_context_to_csv()



