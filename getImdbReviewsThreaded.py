import pandas as pd
import time
import random
from csv import DictReader
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from shadow_useragent import ShadowUserAgent
from urllib3.exceptions import NewConnectionError

start_time = time.time()

MAX_THREADS = 5
processsed = 0
shadow_useragent = ShadowUserAgent()
timeDelays = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
reviews = []
options = Options()
options.add_argument('--log-level=3')


def formatData(data):
    return datetime.strptime(data, "%d %B %Y").strftime('%Y-%m-%d')

service = Service('utils/chromedriver')
service.start()

totalCountCSV = 0

def flatCSV(filehandler):
    global totalCountCSV
    title, cast = [], []

    csvReader = DictReader(filehandler)
    # with alive_bar(250000) as bar:
    # bar = alive_it(csvReader, total=250000)
    for row in csvReader:
        title.append(row['Title'])
        totalCountCSV += 1
        cast.append(row["Cast"].split(", ")[0])
    filehandler.close()
    return title, cast


def getImdbReviews(title, cast):

    global reviews, totalCountCSV, processsed
    print(f'Scrapping {title} {processsed + 1}/{totalCountCSV + 1}')
    driver = webdriver.Remote(service.service_url, options=options)
    driver.maximize_window()
    driver.get('https://www.imdb.com/search/title/')
    try:
        titleBar = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//div[@class="inputs"]/input[@name="title"]')))
        titleBar.send_keys(title)
        castBar = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'Name-search-bar-input')))
        castBar.send_keys(cast)
        castBarRes = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'Name-search-bar-results')))
        time.sleep(3)
        castBarRes = castBarRes.find_element(By.XPATH, '//a[@class="search_item"]')
        castBarRes.click()
        titleBar.send_keys(Keys.ENTER)
        time.sleep(3)
        movieLink = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//div[@class="lister-item-content"]/h3/a')))
        #
        time.sleep(3)
        # link = movieLink.get_attribute('href')
        movieLink.click()
        time.sleep(3)
        print('tmm 1')
        reviewsLink = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//div[@class="sc-f4578077-5 cOGCSE"]/ul/li[2]/a')))
        print('tmm 2')
        reviewsLink.click()
        print('tmm 3')


        time.sleep(3)

        reviewsCount = 25


        loadMore = driver.find_element(By.XPATH, "//button[@class='ipl-load-more__button']")
        # print("Scrolling")
        print(driver.execute_script("return document.querySelector('.header span').textContent").split()[0])
        while int(
                driver.execute_script(
                    "return Array.from(document.querySelectorAll('#main .review-container')).length")) < int(
            driver.execute_script("return document.querySelector('.header span').textContent").split()[0].replace(",",
                                                                                                                  "")):
            driver.execute_script('document.querySelector(".ipl-load-more__button").click()')
            time.sleep(1)
            reviewsCount += 25
            if reviewsCount >= 250:
                break
        # print("End Scrolling")
        seriesTitle = driver.find_element(By.XPATH, "//div[@class='parent']/h3/a").text
        titles = driver.find_elements(By.XPATH, "//a[@class='title']")
        elements = driver.find_elements(By.XPATH, "//div[@class='content']")
        authors = driver.find_elements(By.XPATH, "//span[@class='display-name-link']/a")
        dates = driver.find_elements(By.XPATH, "//span[@class='review-date']")
        currUrl = driver.current_url.replace("/reviews/?ref_=tt_ql_urv", "")
        print(f'Scrapping: {seriesTitle}')
        for title, review, author, date in zip(titles, elements, authors, dates):
            # print("<start review>")
            # print(f'Date: {formatData(date.text)} Author: {author.text}')
            # print(title.text.replace("\n", " "))
            # print(review.text.replace("\n", " "))
            # print("<end review>")
            reviews.append(
                [currUrl, seriesTitle, title.text.replace("\n", " "),
                 review.text.replace("\n", " "), author.text, formatData(date.text)])
        print("--- %s seconds ---" % (time.time() - start_time))

        time.sleep(random.choice(timeDelays) * 2)

    except Exception as e:
        print(e)
        reviewsDF = pd.DataFrame(reviews)
        reviewsDF.columns = ['url', 'movieName', 'review_title', 'review_text', 'review_author', 'review_date']
        reviewsDF.to_csv('testRR.csv', index=False)
    finally:
        processsed += 1
        driver.quit()


def main(titleList, castList):
    global reviews
    threads = min(MAX_THREADS, len(titleList))
    with ThreadPoolExecutor(max_workers=threads) as executor:

        executor.map(getImdbReviews, titleList, castList)

try:
    title, cast = flatCSV(open('data/partialReviews/1.csv', 'r'))
    main(title, cast)

except Exception as e:
    reviewsDF = pd.DataFrame(reviews)
    reviewsDF.columns = ['url', 'movieName', 'review_title', 'review_text', 'review_author', 'review_date']
    reviewsDF.to_csv('data/partialReviews/1.res.csv', index=False)

except KeyboardInterrupt:
    reviewsDF = pd.DataFrame(reviews)
    reviewsDF.columns = ['url', 'movieName', 'review_title', 'review_text', 'review_author', 'review_date']
    reviewsDF.to_csv('data/partialReviews/1.res.csv', index=False)

except NewConnectionError as nce:
    print(f'New connection error: {nce}')
    reviewsDF = pd.DataFrame(reviews)
    reviewsDF.columns = ['url', 'movieName', 'review_title', 'review_text', 'review_author', 'review_date']
    reviewsDF.to_csv('data/partialReviews/1.res.csv', index=False)

finally:
    reviewsDF = pd.DataFrame(reviews)
    reviewsDF.columns = ['url', 'movieName', 'review_title', 'review_text', 'review_author', 'review_date']
    reviewsDF.to_csv('data/partialReviews/1.res.csv', index=False)