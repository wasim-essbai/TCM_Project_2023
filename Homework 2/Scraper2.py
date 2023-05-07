import time
import pandas as pd
from selenium import webdriver as wd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.support.wait import WebDriverWait

max_page = 125
sleep_time = 1
log = False

chromedriver_path = '/Users/Wasim/Downloads/chromedriver/chromedriver'


def get_browser():
    chrome_options = wd.ChromeOptions()
    chrome_options.add_argument('log-level=3')
    return wd.Chrome(chromedriver_path, options=chrome_options)


browser = get_browser()


def talk_is_in_list(ted_talk, list_to_check):
    idx_talk = ted_talk['idx']
    for el in list_to_check:
        if el['idx'] == idx_talk:
            return True
    return False


def get_talks_num_likes(ted_talk):
    if log:
        print("Current url: " + ted_talk['url'])

    try:
        browser.get(ted_talk['url'])
        # Video doesn't exists
        if browser.title == "TED | 404: Not Found":
            raise Exception('Talk not available')

        #time.sleep(sleep_time)
        # ensures like number is loaded on the page
        like_element = WebDriverWait(browser, 5) \
            .until(EC.presence_of_element_located((By.XPATH,
                                                   "//div[@class='transition-opacity duration-300 inline-flex "
                                                   "items-center opacity-100']//span")))

        talk_num_likes = like_element.text.strip()[1:-1]
        return talk_num_likes
    except Exception as err:
        # print(err)
        return "-1"


talks_df = pd.read_csv("./data/tedx_dataset_clean.csv")
talks_list = talks_df.to_dict('records')

num_likes_df = pd.read_csv("./data/num_likes_dataset.csv")
num_likes_list = num_likes_df.to_dict('records')

for talk in talks_list:
    if not talk_is_in_list(talk, num_likes_list):
        num_likes = get_talks_num_likes(talk)
        if num_likes != "-1":
            num_likes_list.append({"idx": talk['idx'], "num_likes": num_likes})

print(f'Number of retrieved talks number of likes: {len(num_likes_list)}')

num_likes_dataset_df = pd.DataFrame.from_dict(num_likes_list)
num_likes_dataset_df.to_csv('./data/num_likes_dataset.csv', index=False)
