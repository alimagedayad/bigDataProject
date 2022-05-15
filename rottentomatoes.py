import lxml
import cchardet
import pandas as pd
import time
import customExceptions
from datetime import datetime
from urllib3 import PoolManager, Retry, exceptions
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager
from unidecode import unidecode
from csv import DictReader
from bs4 import BeautifulSoup
from alive_progress import alive_it
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem

software_names = [SoftwareName.CHROME.value]
operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)


# actorsDf = pd.DataFrame()
# dataDf = pd.DataFrame()
#
# actorsDf.columns = [",Picture,Name,Date of Birth,Birthplace"]
# dataDf.columns = [",Tomatometer,Audience Score,Title,Character,Box Office,Year,Cast"]


def formatData(data):
    return datetime.strptime(data, "%b %d, %Y").strftime('%Y-%m-%d')


def removeAccents(name):
    return unidecode(name, "utf-8")


retries = Retry(connect=5, read=1, redirect=1, backoff_factor=.2)
pool = PoolManager(retries=retries)

# driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
# driver.maximize_window()
# print(f'Random user agent: ', user_agent_rotator.get_random_user_agent())


# headers = {'User-Agent': user_agent_rotator.get_random_user_agent()}

def scrapeRotten(filename):
    actors = []
    data = []
    headers = {'User-Agent': user_agent_rotator.get_random_user_agent()}
    with open(filename, 'r') as readObj:
        csvReader = DictReader(readObj)
        # with alive_bar(250000) as bar:
        bar = alive_it(csvReader, total=234454)
        for row in bar:
            urlSegment = removeAccents(row['actors'])
            url = f"https://www.rottentomatoes.com/celebrity/{urlSegment}"
            try:
                bar.text(f'Getting {urlSegment} data')
                page = pool.request('GET', url, headers=headers)

                if(page.status != 200 and page.status != 404):
                    print(f'User agents exhausted [{page.status}]')
                    raise customExceptions.UserAgentsExhaustion
                elif page.status == 404:
                    continue
                soup = BeautifulSoup(page.data, 'lxml')

                # print(soup)

                actorName = soup.find('h1', attrs={'class': 'celebrity-bio__h1'}).text.strip()
                actorPic = soup.find('img', attrs={'class': 'celebrity-bio__hero-img'})['data-src']
                actorBirthday = soup.find('p', attrs={'data-qa': 'celebrity-bio-bday'}) \
                    .text.strip() \
                    .replace("\n", "") \
                    .replace("Birthday: ", ""). \
                    strip()

                actorBirthplace = soup.find('p', attrs={'data-qa': 'celebrity-bio-birthplace'}) \
                    .text.strip() \
                    .replace("\n", "") \
                    .replace("Birthplace: ", ""). \
                    strip()

                actors.append([actorPic, actorName, actorBirthday, actorBirthplace])

                # print(actors)

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


            except AttributeError:
                print(f'Missed: {urlSegment} Last actor: {actors[-1][1]} Last film: {data[-1][2]}')
                continue
            except KeyboardInterrupt:
                print('Saving data before skipping [KI]')
                actorsDF = pd.DataFrame(actors)
                actorsDF.columns = ['Picture', 'Name', 'Date of Birth', 'Birthplace']
                actorsDF.to_csv(f'data/actorsData.csv')

                moviesDF = pd.DataFrame(data)
                moviesDF.columns = ['Tomatometer', 'Audience Score', 'Title', 'Character', 'Box Office', 'Year', 'Cast']
                moviesDF.to_csv(f'data/tvData.csv')
                continue
            except exceptions.NewConnectionError:
                print("Failed to create new connection")
                time.sleep(60)
                continue
            except exceptions.ConnectTimeoutError:
                print("Connection Timeout")
                continue
            except exceptions.MaxRetryError:
                print('Max retries for this connections are exhausted. Sleeping for 10mins (May the odds be ever in your favor)' )
                time.sleep(60 * 10)
                continue
            except customExceptions.UserAgentsExhaustion:
                print("User agents are exhausted. Try to change your ip. Sleeping for 10mins")
                time.sleep(60 * 10)
                continue
            except Exception:
                actorsDF = pd.DataFrame(actors)
                actorsDF.columns = ['Picture', 'Name', 'Date of Birth', 'Birthplace']
                actorsDF.to_csv(f'data/actorsData.csv')

                moviesDF = pd.DataFrame(data)
                moviesDF.columns = ['Tomatometer', 'Audience Score', 'Title', 'Character', 'Box Office', 'Year', 'Cast']
                moviesDF.to_csv(f'data/tvData.csv')
                continue

    actorsDF = pd.DataFrame(actors)
    actorsDF.columns = ['Picture', 'Name', 'Date of Birth', 'Birthplace']
    actorsDF.to_csv(f'data/actorsData.csv')

    moviesDF = pd.DataFrame(data)
    moviesDF.columns = ['Tomatometer', 'Audience Score', 'Title', 'Character', 'Box Office', 'Year', 'Cast']
    moviesDF.to_csv(f'data/tvData.csv')


scrapeRotten('actors.csv')

