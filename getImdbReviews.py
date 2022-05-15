from csv import DictReader
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementClickInterceptedException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
from alive_progress import alive_it, alive_bar
import pandas as pd
import time

start_time = time.time()
totalCountCSV = 0

service = Service('utils/chromedriver')
service.start()

reviews = []

driver = webdriver.Remote(service.service_url)
driver.maximize_window()

actualReviewsCount = 0


def flatCSV(filehandler):
    global totalCountCSV
    titles, casts = [], []
    csvReader = DictReader(filehandler)
    # with alive_bar(250000) as bar:
    # bar = alive_it(csvReader, total=250000)
    for row in csvReader:
        titles.append(row['Title'])
        totalCountCSV += 1
        casts.append(row["Cast"].split(", ")[0])
    filehandler.close()
    return titles, casts


def formatData(data):
    return datetime.strptime(data, "%d %B %Y").strftime('%Y-%m-%d')


def sub(titlesList, castList):
    global actualReviewsCount
    with alive_bar(len(titlesList), title=f'Scrapping ') as bar:
        for title, cast in zip(titlesList, castList):
            try:
                bar.text = f'-> Scraping: {title} feat. {cast}'
                driver.get('https://www.imdb.com/search/title/')

                titleBar = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@class="inputs"]/input[@name="title"]')))
                titleBar.send_keys(title)
                castBar = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.ID, 'Name-search-bar-input')))
                castBar.send_keys(cast)
                castBarRes = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.ID, 'Name-search-bar-results')))
                time.sleep(3)
                castBarRes = castBarRes.find_element(By.XPATH, '//a[@class="search_item"]')
                castBarRes.click()
                titleBar.send_keys(Keys.ENTER)
                time.sleep(3)
                movieLink = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@class="lister-item-content"]/h3/a')))
                #
                time.sleep(3)
                # link = movieLink.get_attribute('href')
                movieLink.click()
                time.sleep(3)

                reviewsLink = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@class="sc-f4578077-5 cOGCSE"]/ul/li[2]/a')))
                reviewsLink.click()
                time.sleep(3)
                reviewsCount = 25
                loadMore = driver.find_element(By.XPATH, "//button[@class='ipl-load-more__button']")
                # print("Scrolling")
                print(driver.execute_script("return document.querySelector('.header span').textContent").split()[0])
                while int(
                        driver.execute_script(
                            "return Array.from(document.querySelectorAll('#main .review-container')).length")) < int(
                    driver.execute_script("return document.querySelector('.header span').textContent").split()[
                        0].replace(",",
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
                for r_title, review, author, date in zip(titles, elements, authors, dates):
                    actualReviewsCount += 1
                    print("<start review>")
                    # print(f'Date: {formatData(date.text)} Author: {author.text}')
                    # print(title.text.replace("\n", " "))
                    print(review.text.replace("\n", " "))
                    print("<end review>")
                    reviews.append(
                        [currUrl, seriesTitle, r_title.text.replace("\n", " "),
                         review.text.replace("\n", " "), author.text, formatData(date.text)])
                print(f'Scrapped: {seriesTitle} in {time.time() - start_time} seconds')
                time.sleep(10)
            except EOFError:
                print('Keyboard Interrupt')
                if len(reviews) > 0:
                    reviewsDF = pd.DataFrame(reviews)
                    reviewsDF.columns = ['url', 'movieName', 'review_title', 'review_text', 'review_author', 'review_date']
                    reviewsDF.to_csv('data/partialReviews/2.res.csv', index=False)
                driver.quit()
                continue

            except TimeoutException:
                print(f'{title} timed out')
                if len(reviews) > 0:
                    reviewsDF = pd.DataFrame(reviews)
                    reviewsDF.columns = ['url', 'movieName', 'review_title', 'review_text', 'review_author', 'review_date']
                    reviewsDF.to_csv('data/partialReviews/2.res.csv', index=False)
                continue
            except NoSuchElementException:
                print(f'{title} no such element')
                if len(reviews) > 0:
                    reviewsDF = pd.DataFrame(reviews)
                    reviewsDF.columns = ['url', 'movieName', 'review_title', 'review_text', 'review_author',
                                         'review_date']
                    reviewsDF.to_csv('data/partialReviews/2.res.csv', index=False)
                continue
            except ElementClickInterceptedException:
                print(f'{title} element click intercepted')
                if len(reviews) > 0:
                    reviewsDF = pd.DataFrame(reviews)
                    reviewsDF.columns = ['url', 'movieName', 'review_title', 'review_text', 'review_author',
                                         'review_date']
                    reviewsDF.to_csv('data/partialReviews/2.res.csv', index=False)
                continue
            except StaleElementReferenceException:
                print(f'{title} stale element')
                if len(reviews) > 0:
                    reviewsDF = pd.DataFrame(reviews)
                    reviewsDF.columns = ['url', 'movieName', 'review_title', 'review_text', 'review_author',
                                         'review_date']
                    reviewsDF.to_csv('data/partialReviews/2.res.csv', index=False)
                continue

            except Exception as e:
                print(f'Caught {e}')
                if len(reviews) > 0:
                    reviewsDF = pd.DataFrame(reviews)
                    reviewsDF.columns = ['url', 'movieName', 'review_title', 'review_text', 'review_author', 'review_date']
                    reviewsDF.to_csv('data/partialReviews/2.res.csv', index=False)
                continue

            finally:
                time.sleep(.02)
                bar()
        # NoSuchElementException, TimeoutException, ElementClickInterceptedException, StaleElementReferenceException

    print(f"Finished scrapping {actualReviewsCount} reviews")
    reviewsDF = pd.DataFrame(reviews)
    reviewsDF.columns = ['url', 'movieName', 'review_title', 'review_text', 'review_author', 'review_date']
    reviewsDF.to_csv('data/partialReviews/2.res.csv', index=False)


# def getReview(title, cast):
#     global reviews, totalCountCSV, actualReviewsCount
#
#     try:
#         driver.get('https://www.imdb.com/search/title/')
#
#         titleBar = WebDriverWait(driver, 10).until(
#             EC.presence_of_element_located((By.XPATH, '//div[@class="inputs"]/input[@name="title"]')))
#         titleBar.send_keys(title)
#         castBar = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'Name-search-bar-input')))
#         castBar.send_keys(cast)
#         castBarRes = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'Name-search-bar-results')))
#         time.sleep(3)
#         castBarRes = castBarRes.find_element(By.XPATH, '//a[@class="search_item"]')
#         castBarRes.click()
#         titleBar.send_keys(Keys.ENTER)
#         time.sleep(3)
#         movieLink = WebDriverWait(driver, 10).until(
#             EC.presence_of_element_located((By.XPATH, '//div[@class="lister-item-content"]/h3/a')))
#         #
#         time.sleep(3)
#         # link = movieLink.get_attribute('href')
#         movieLink.click()
#         time.sleep(3)
#         reviewsLink = WebDriverWait(driver, 10).until(
#             EC.presence_of_element_located((By.XPATH, '//div[@class="sc-f4578077-5 cOGCSE"]/ul/li[2]/a')))
#         reviewsLink.click()
#         time.sleep(3)
#         reviewsCount = 25
#         loadMore = driver.find_element(By.XPATH, "//button[@class='ipl-load-more__button']")
#         # print("Scrolling")
#         print(driver.execute_script("return document.querySelector('.header span').textContent").split()[0])
#         while int(
#                 driver.execute_script(
#                     "return Array.from(document.querySelectorAll('#main .review-container')).length")) < int(
#             driver.execute_script("return document.querySelector('.header span').textContent").split()[0].replace(",",
#                                                                                                                   "")):
#             driver.execute_script('document.querySelector(".ipl-load-more__button").click()')
#             time.sleep(1)
#             reviewsCount += 25
#             if reviewsCount >= 250:
#                 break
#         # print("End Scrolling")
#         seriesTitle = driver.find_element(By.XPATH, "//div[@class='parent']/h3/a").text
#         titles = driver.find_elements(By.XPATH, "//a[@class='title']")
#         elements = driver.find_elements(By.XPATH, "//div[@class='content']")
#         authors = driver.find_elements(By.XPATH, "//span[@class='display-name-link']/a")
#         dates = driver.find_elements(By.XPATH, "//span[@class='review-date']")
#         currUrl = driver.current_url.replace("/reviews/?ref_=tt_ql_urv", "")
#         print(f'Scrapping: {seriesTitle}')
#         for title, review, author, date in zip(titles, elements, authors, dates):
#             actualReviewsCount += 1
#             # print("<start review>")
#             # print(f'Date: {formatData(date.text)} Author: {author.text}')
#             # print(title.text.replace("\n", " "))
#             # print(review.text.replace("\n", " "))
#             # print("<end review>")
#             reviews.append(
#                 [currUrl, seriesTitle, title.text.replace("\n", " "),
#                  review.text.replace("\n", " "), author.text, formatData(date.text)])
#         print(f'Scrapped: {seriesTitle} in {time.time() - start_time} seconds')
#         time.sleep(10)
#     except Exception as e:
#         raise e


title, cast = flatCSV(open('data/partialReviews/1.csv', 'r'))
sub(title, cast)

