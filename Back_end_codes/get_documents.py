import pyodbc
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
from selenium import webdriver
import uuid

# import necessary classes
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


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

# executing SQL statement
# query = """
# SELECT * FROM QUESTION q LEFT JOIN QUESTION_ANSWER qa ON q.q_id = qa.q_id
# WHERE qa.qa_id IS NULL;
# """
query = """
SELECT * FROM QUESTION q LEFT JOIN QUESTION_ANSWER qa ON q.q_id = qa.q_id
WHERE q.q_id = 'CQ051';
"""
cursor.execute(query)

row = cursor.fetchone()
while row:
    baseUrl = 'https://www.google.com/search?q='
    plusUrl = row[1]
    url = baseUrl + quote_plus(plusUrl)

    # chromedriver path input
    driver = webdriver.Chrome('C:\chromedriver_win32\chromedriver')
    driver.get(url)
    driver.implicitly_wait(10)

    html = driver.page_source
    soup = BeautifulSoup(html)

    i = soup.select_one('.V3FYCf')
    if i is not None:
        answer = i.contents[0].text
        doc_id = str(uuid.uuid1().hex)   # Generate Random UUID
        doc_url = i.a.attrs['href']
        doc_title = i.select_one('.LC20lb.DKV0Md').text

        sql_doc_str = "INSERT INTO DOCUMENT (doc_id, doc_title, doc_url) VALUES ('" + doc_id + "', '" + doc_title + "', '" + doc_url + "');"
        print(sql_doc_str)

        sql_qa_str = "INSERT INTO QUESTION_ANSWER (q_id, answer_text, doc_id) VALUES ('" + row[0] + "', '" + answer + "', '" + doc_id + "');"
        print(sql_qa_str)
    else:
        print(row[0] + " - !!No Main Answer Found for this question!!")
    row = cursor.fetchone()
    print("-----------------------")


driver.close()
cnxn.close()