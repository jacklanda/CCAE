# -*- coding: utf-8 -*-
# @author: YangLiu
# @email: yangliu.real@gmail.com

# Google advanced search crawler
# use each trigram as query to search
# and collect each page of results.

import sys
from time import time, sleep
from random import sample, randint
from typing import List

from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from utils import req_2captcha


# This is not needed if chromedriver is already on your path:
CHROMEDRIVER_PATH = "chromedriver"  # set the path of chromedriver
SERVICE = Service(CHROMEDRIVER_PATH)

OPTIONS = Options()
OPTIONS.add_argument("--window-size=1920x1080")
OPTIONS.add_argument("--verbose")
OPTIONS.add_argument("--disable-javascript")
OPTIONS.add_argument('--no-sandbox')
OPTIONS.add_argument("--headless")

CC_MAP = {"cn": "CN", "hk": "HK", "mo": "MO", "tw": "TW", "my": "MY", "sg": "SG"}
AREA = CC_MAP[sys.argv[1]]
TEMPLATE = "https://www.google.com/search?hl=zh-CN&as_q=[QUERY]+filetype%3Ahtml&as_epq=&as_oq=&as_eq=&as_nlo=&as_nhi=&lr=lang_en&cr=country[AREA]&as_qdr=all&as_sitesearch=&as_occt=any&safe=images&as_filetype=&tbs="


def read_query_file(fpath: str = "data/query.txt"):
    with open(fpath, "r") as f:
        query_list = f.readlines()
        query_list = [q.strip() for q in query_list]
    # return query_list
    return sample(query_list, k=5000)


def append2csv(url_list: List):
    with open(f"./url.{sys.argv[1]}.csv", "a+") as f:
        print(f"save url nums: {len(url_list)}")
        for url in url_list:
            f.write(url + '\n')


def extract_urls(driver: WebDriver):
    url_list = list()
    elems = driver.find_elements(By.XPATH, "//div[@class='yuRUbf']/a")
    for elem in elems:
        url = elem.get_attribute("href")
        url_list.append(url)
    return url_list


def scroll_and_extract(driver: WebDriver):
    sleep_time = randint(1, 3)
    try:
        driver.find_element(by=By.CSS_SELECTOR, value="#pnnext > span:nth-child(2)").click()
    except Exception as e:
        raise(e)
    else:
        sleep(sleep_time)
        return extract_urls(driver)
    return list()


def solve_recaptcha(driver: WebDriver):
    res = driver.find_elements(By.CLASS_NAME, "g-recaptcha")
    if len(res) > 0:
        url = driver.current_url
        sitekey = res[0].get_attribute("data-sitekey")
        data_s = res[0].get_attribute("data-s")
        cookies = ""
        cookies_list = driver.get_cookies()
        for item in cookies_list:
            for k, v in item.items():
                cookies += str(k) + ':' + str(v) + ';'
        cookies = cookies.rsplit(';', 1)[0]
        print("appears reCaptcha")
        # print(f"current_url: {url}")
        # print(f"sitekey: {sitekey}")
        # print(f"data-s: {data_s}")
        # print(f"cookies: {cookies}")
        try:
            token = req_2captcha(url=url, sitekey=sitekey, data_s=data_s, cookies=cookies, proxy=None)
        except:
            sleep(30)
            return
        # print(f"token: {token}")
        print("bypass reCaptcha succ")
        redirect_url = url + "&g-recaptcha-response=" + token
        print("request a url using a query")
        driver.get(redirect_url)


def multitab_scroll_extract(driver: WebDriver, batch=None):
    bgn_time = time()
    total_url_nums = 0
    query_list = batch
    url_list = list()

    for i in range(6):
        driver.execute_script(f"window.open('https://www.google.com','tab_{i+1}');")
        driver.switch_to.window(f"tab_{i+1}")

    for w in driver.window_handles:
        query = query_list.pop()
        print("request a url using a query")
        driver.get(TEMPLATE.replace("[QUERY]", query).replace("[AREA]", AREA))
        solve_recaptcha(driver)
        driver.switch_to.window(w)

    time_count = 0
    while query_list:
        count = 0
        sleep_time = randint(1, 3)
        for i, w in enumerate(driver.window_handles, start=1):
            if i == 1:
                solve_recaptcha(driver)
                driver.switch_to.window(w)
            if time_count < 6:
                solve_recaptcha(driver)
                new_urls = extract_urls(driver)
                url_list += new_urls
                print('*' * 100)
                print(f"found new urls: {len(new_urls)}")
                time_count += 1
                sleep(sleep_time)
                continue
            try:
                new_urls = scroll_and_extract(driver)
            except:
                if not query_list:
                    append2csv(url_list)
                    # driver.quit()  # unawailable on linux
                    return
                query = query_list.pop()
                print(f"update new query: {query}")
                print("request a url using a query")
                driver.get(TEMPLATE.replace("[QUERY]", query).replace("[AREA]", AREA))
                solve_recaptcha(driver)
                driver.switch_to.window(w)
                continue
            print('*' * 100)
            print(f"found new urls: {len(new_urls)}")
            url_list += new_urls
            count += len(new_urls)
            solve_recaptcha(driver)
            driver.switch_to.window(w)
            sleep(sleep_time)
        if count == 0:
            if not query_list:
                append2csv(url_list)
                # driver.quit()  # unawailable on linux
                return
            query = query_list.pop()
            print("request a url using a query")
            driver.get(TEMPLATE.replace("[QUERY]", query).replace("[AREA]", AREA))
            solve_recaptcha(driver)
            driver.switch_to.window(w)
        end_time = time()
        total_url_nums = len(url_list)
        print(f"total running time: {round(end_time-bgn_time)}s, total crawled urls: {total_url_nums}")
    append2csv(url_list)
    # driver.quit()  # unawailable on linux


def main(batch=None):
    driver = webdriver.Chrome(service=SERVICE, options=OPTIONS)
    driver.get("https://www.google.com")
    multitab_scroll_extract(driver, batch)


if __name__ == "__main__":
    main()
