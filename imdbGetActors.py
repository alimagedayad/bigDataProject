from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver.maximize_window()

actors = []


def usernamizeName(string):
    string = string.lower().replace("'", "")
    splittedString = string.split()
    if len(splittedString) > 2:
        return splittedString[0] + "_" + splittedString[-1]
    else:
        return string.replace(" ", "_")


# def base64Pagination(start, end):
#     message = f"[{end},\"nm10135221\",{start}]"
#     return str(base64.b64encode(message.encode('ascii'))).replace('b', '').replace("'", "")
#
# urlSegment = None
url = "https://www.imdb.com/search/name/?gender=male,female"
driver.get(url)
while True:
    elements = driver.find_elements(By.XPATH, "//h3[@class='lister-item-header']/a")
    actors.extend([usernamizeName(element.text) for element in elements])

    pageNumber = driver.find_element(By.XPATH, "//div[@class='desc']/span").text.split(" ")[0].split("-")[1].replace(
        ",", "")
    nextButton = driver.find_element(By.XPATH, "//div[@class='desc']/a[last()]")
    print(f"page number: {pageNumber}")
    if int(pageNumber) == 250000:
        break
    else:
        nextButton.click()

reviewsDf = pd.DataFrame({'actors': actors}, index=range(1, len(actors) + 1))
reviewsDf.to_csv('actors.csv')
