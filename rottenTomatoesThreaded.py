import time
import pandas as pd
import urllib3
from random import choice
# from slitherlib.slither import Snake
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from csv import DictReader
from unidecode import unidecode
from datetime import datetime
from shadow_useragent import ShadowUserAgent
from tqdm import tqdm

MAX_THREADS = 30
USER_AGENT_SCRAPER_BASE_URL = 'http://www.useragentstring.com/pages/useragentstring.php?name='
POPULAR_BROWSERS = ['Chrome', 'Firefox', 'Mozilla', 'Safari', 'Opera', 'Opera Mini', 'Edge', 'Internet Explorer']

shadow_useragent = ShadowUserAgent()
data = []
actors = []
timeDelays = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
tot = 0

def removeAccents(name):
    return unidecode(name, "utf-8")


def formatData(data):
    return datetime.strptime(data, "%b %d, %Y").strftime('%Y-%m-%d')


def flatCSV(filehandler):
    tempData = []
    csvReader = DictReader(filehandler)
    # with alive_bar(250000) as bar:
    # bar = alive_it(csvReader, total=250000)
    for row in csvReader:
        tempData.append(removeAccents(row['actors']))
    filehandler.close()

    return tempData


def get_user_agent_strings_for_this_browser(browser):
    """
    Get the latest User-Agent strings of the given Browser
    :param browser: string of given Browser
    :return: list of User agents of the given Browser
    """

    url = USER_AGENT_SCRAPER_BASE_URL + browser
    response = pool.request('GET', url)
    soup = BeautifulSoup(response.data, 'html.parser')
    user_agent_links = soup.find('div', {'id': 'liste'}).findAll('a')[:20]

    return [str(user_agent.text) for user_agent in user_agent_links]


def get_user_agents():
    """
    Gather a list of some active User-Agent strings from
    http://www.useragentstring.com of some of the Popular Browsers
    :return: list of User-Agent strings
    """

    user_agents = []
    for browser in POPULAR_BROWSERS:
        user_agents.extend(get_user_agent_strings_for_this_browser(browser))
    return user_agents[3:]  # Remove the first 3 Google Header texts from Chrome's user agents


pool = urllib3.PoolManager()
uas = get_user_agents()


def getDataFromRotten(actor):
    global actors, data, tot
    headers = {'User-Agent': choice(uas)}
    url = f"https://www.rottentomatoes.com/celebrity/{actor}"

    # print(f'getDataFromRotten: {actor}')

    try:

        page = pool.request('GET', url, headers=headers)

        soup = BeautifulSoup(page.data, 'html.parser')
        actorName = soup.find('h1', attrs={'class': 'celebrity-bio__h1'}).text.strip()

        actorPic = soup.find('img', attrs={'class': 'celebrity-bio__hero-img'})['data-src']
        actorBirthday = soup.find('p', attrs={'data-qa': 'celebrity-bio-bday'}) \
            .text.strip() \
            .replace("\n", "") \
            .replace("Birthday: ", ""). \
            strip()

        actorBirthday = formatData(actorBirthday)

        actorBirthplace = soup.find('p', attrs={'data-qa': 'celebrity-bio-birthplace'}) \
            .text.strip() \
            .replace("\n", "") \
            .replace("Birthplace: ", ""). \
            strip()

        actors.append([actorPic, actorName, actorBirthday, actorBirthplace])

        moviesTable = soup.find('table', attrs={'data-qa': 'celebrity-filmography-movies'})
        moviesTableBody = moviesTable.find('tbody')
        rows = moviesTableBody.find_all('tr')

        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip().replace("\n", "").strip() for ele in cols]

            if len(cols) == 5:
                cols.insert(4, '-')

            temp = [element for element in cols if element]
            temp.append(actorName)
            data.append(temp)

        showTable = soup.find('table', attrs={'data-qa': 'celebrity-filmography-tv'})
        showTableBody = showTable.find('tbody')
        rows = showTableBody.find_all('tr')

        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip().replace("\n", "").strip() for ele in cols]

            if len(cols) == 5:
                cols.insert(4, '-')

            temp = [element for element in cols if element]
            temp.append(actorName)
            data.append(temp)
        time.sleep(choice(timeDelays))
        print(f'Finished Processing: {tot}/250,000', [actorName, actorBirthday])
        tot += 1
        return actorName
        # if urlSegment == 'rachael_harris':
        #     break

    except AttributeError:
        print(f'Missed: {actor} Last actor: {actors[-1][1]} Last film: {actor[-1][2]}')


def scrape(actorList):
    global actors, data
    threads = min(MAX_THREADS, len(actorList))

    # with tqdm(total=250000) as pbar:
    #     with ThreadPoolExecutor(max_workers=threads) as executor:
    #         futures = [executor.submit(getDataFromRotten, name) for name in actorList]
    #         for future in as_completed(futures):
    #             result = future.result()
    #             pbar.set_description(f"Finished processing: {result}")
    #             pbar.update(1)

    with ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(getDataFromRotten, actorList)

    # print(actors)

    # actorsDF = pd.DataFrame(actors)
    # actorsDF.columns = ['Picture', 'Name', 'Date of Birth', 'Birthplace']
    # actorsDF.to_csv(f'data/actorsInfo.csv')
    #
    # moviesDF = pd.DataFrame(data)
    # moviesDF.columns = ['Tomatometer', 'Audience Score', 'Title', 'Character', 'Box Office', 'Year', 'Cast']
    # actorsDF.to_csv(f'data/moviesInfo.csv')


actorsList = flatCSV(open('actors.csv', 'r'))

scrape(actorsList)

actorsDF = pd.DataFrame(actors)
actorsDF.columns = ['Picture', 'Name', 'Date of Birth', 'Birthplace']
actorsDF.to_csv(f'data/actorsDataset.csv')

moviesDF = pd.DataFrame(data)
moviesDF.columns = ['Tomatometer', 'Audience Score', 'Title', 'Character', 'Box Office', 'Year', 'Cast']
moviesDF.to_csv(f'data/rtShows.csv')
