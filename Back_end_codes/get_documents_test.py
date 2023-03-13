import pyodbc
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
from selenium import webdriver
import uuid

# import necessary classes
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



user_input = input("Enter : ")
baseUrl = 'https://www.google.com/search?q='
plusUrl = user_input
url = baseUrl + quote_plus(plusUrl)

# chromedriver path input
driver = webdriver.Chrome('C:\chromedriver_win32\chromedriver')
driver.get(url)
driver.implicitly_wait(5)

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

    sql_qa_str = "INSERT INTO QUESTION_ANSWER (q_id, answer_text, doc_id) VALUES ('" + user_input + "', '" + answer + "', '" + doc_id + "');"
    print(sql_qa_str)
else:
    print(user_input + " - !!No Main Answer Found for this question!!")
print("-----------------------")


driver.close()