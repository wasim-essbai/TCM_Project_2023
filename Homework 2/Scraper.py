import pandas as pd
from selenium import webdriver as wd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.support.wait import WebDriverWait

max_page = 125
sleep_time = 1
log = False

chromedriver_path = '/Users/Wasim/Downloads/chromedriver/chromedriver'
#chromedriver_path = 's3://unibg-tcm-tedx-pausaattiva/data'

URL = 'https://www.ted.com/talks'


def get_browser():
    chrome_options = wd.ChromeOptions()
    chrome_options.add_argument('log-level=3')
    return wd.Chrome(chromedriver_path, options=chrome_options)


browser = get_browser()


def get_talks_duration(ted_talk):
    if log:
        print("Current url: " + ted_talk['url'])

    try:
        browser.get(ted_talk['url'])
        # Video doesn't exists
        if browser.title == "TED | 404: Not Found":
            raise Exception('Transcript not available')

        # ensures ted player is loaded on the page
        player = WebDriverWait(browser, 5).until(EC.presence_of_element_located((By.ID, "ted-player")))

        # get the ted playaer
        duration_text = browser.find_element(By.XPATH, "//div[@id='ted-player']//span[@class = 'css-1hg238t']").text
        minutes = int(duration_text.strip().split("/")[1].split(":")[0])
        seconds = int(duration_text.strip().split("/")[1].split(":")[1])
        duration_sec = minutes * 60 + seconds

        return duration_sec
    except Exception as err:
        print(err)
        return -1


talks_df = pd.read_csv("./data/tedx_dataset.csv")
talks_list = talks_df.to_dict('records')
len(talks_list)

duration_dataset = []
print("Start retrieving durations")
for talk in talks_list:
    duration = get_talks_duration(talk)
    duration_dataset.append({"idx": talk['idx'], "duration": duration})

print("Done")

duration_dataset_df = pd.DataFrame.from_dict(duration_dataset)
duration_dataset_df.to_csv('./data/duration_dataset.csv', index=False)
