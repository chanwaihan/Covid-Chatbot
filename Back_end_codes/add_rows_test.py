import csv
import json
import os
import pyodbc
import glob
import pandas as pd

# Directory to expert articles JSON
document_expert_json_dir = "C:/Users/pckim/Desktop/University_Files/Year3-Sem2/FIT3161-3162/Sem2/Actual_project/covid-chatbot/test_documents"


def get_data_from_filedir(file):
    # Opening JSON file
    f = open(file, "r")

    # returns JSON object as 
    # a dictionary
    data = json.load(f)
    
    # Closing file
    f.close()

    return data


def add_document_expert():
    for file in os.listdir(document_expert_json_dir):
        print("\nTesting " + file)
        if file.endswith(".json"):
            expert_data = get_data_from_filedir(os.path.join(document_expert_json_dir, file)) # Only one dictionary data
            doc_id = expert_data["document_id"]
            title = expert_data["metadata"]["title"]
            authors = expert_data["metadata"]["authors"]
            urls = ';'.join(expert_data["metadata"]["urls"])

            query = ("INSERT INTO DOCUMENT VALUES (" + doc_id + ", " + title + ", " + authors + ", " + urls)
            print(query)
        else:
            print("Ignored")


add_document_expert()