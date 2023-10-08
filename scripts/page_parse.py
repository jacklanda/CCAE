# -*- coding: utf-8 -*-
# @author: YangLiu
# @email: yangliu.real@gmail.com

# Text extractor for web page,
# which depends on pebbel and Justext / GNE,
# respectively for multiprocess parsing
# with timeout utility and text auto extraction.

import os
import re
import sys
import signal
import datetime
from glob import glob
from pickle import load
from dateutil.parser import parse
from typing import Any, Dict, Generator, List, Tuple

# from pebble import ProcessPool  # no need if sequential parsing only
from justext import justext, get_stoplist
from gne import GeneralNewsExtractor as GNE

# country code
CC = "cn"
# data folder which contains pkl files to process
INPUT_FOLDER = f"data/{CC}"
# output tsv file path
OUPUT_PATH = f"data/metadata.raw.{CC}.tsv"


class PageExtractor(object):
    """Page extractor base class"""

    def __init__(self):
        super(PageExtractor).__init__()
        self.extractor = None


class GNEPageExtractor(PageExtractor):
    """Page extractor depends on GNE package"""

    def __init__(self):
        super(GNEPageExtractor, self).__init__()
        self.extractor = GNE()

    def __call__(self, *args: Any, **kwargs: Any) -> Tuple[str, str]:
        res = self.extractor.extract(*args)
        return res["title"], res["publish_time"], res["content"]

    def parse(self, html: str) -> Dict:
        return self.extractor.extract(html)


class JustextPageExtractor(PageExtractor):
    """Page extractor depends on Justext package"""

    def __init__(self):
        super(JustextPageExtractor, self).__init__()
        self.extractor = justext

    def __call__(self, html: str, *args: Any, **kwargs: Any) -> List:
        text_list = list()
        paras = self.extractor(html, get_stoplist("English"))
        for para in paras:
            if para.class_type == "good" or para.class_type == "near-good":
                text_list.append((para.class_type, para.text.strip()))
        return text_list

    def parse(self, html: str) -> List:
        return self.extractor.extract(html)


class Timeout:
    """Timeout class for timing and avoiding long-time string processing."""

    def __init__(self, seconds: int = 1, error_message: str = "Timeout"):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


def preprocess(text: str) -> str:
    """preprocess for input text, e.g., title, content.
    including: 1) remove none-english text, 2) remove html/css/js code,
    3) remove control characters. 4) etc.
    """
    text = re.sub(r'</?[a-zA-Z][^>]*>', '', text)
    text = re.sub(r"[\u4e00-\u9fff]+", "", text)
    return " ".join(text.split())


def calc_word_nums(text: str) -> int:
    """calc words for input text according to space."""
    for punc in '''!()-[]{};:'"\,<>./?@#$%^&*_~''':
        text = text.replace(punc, "")
    word_list = [w for w in text.split() if w and not w.isspace()]
    return len(word_list)


def parse_domain(url: str) -> str:
    """parse domain for input url."""
    return url.split("://", 1)[1].split("/", 1)[0].strip()


def parse_datetime(s: str) -> str:
    """parse datatime to target format."""
    return parse(s).strftime("%Y-%m-%d")


def pkl_loader(fpath: str = INPUT_FOLDER) -> Generator:
    """pickle file loader"""
    for fpath in glob(os.path.join(fpath, "*.pkl")):
        yield (fpath, load(open(file=fpath, mode="rb")))


def item_loader(d: Dict) -> Generator:
    """item loader for url2pair dictionary"""
    for i, item in enumerate(d.items()):
        url = item[0]
        time = item[1][0]
        raw_html = item[1][1]
        yield i, url, time, raw_html


if __name__ == "__main__":

    idx = 0
    ouput_file = open(OUPUT_PATH, "w")
    ouput_file.write("TextID\tTime\tWords\tVariety\tGenre\tDomain\tURL\tTitle\tContent\n")
    parser = GNEPageExtractor()  # use GNE as default page parser

    for pkl_fpath, url2pair in pkl_loader():

        for i, url, time, raw_html in item_loader(url2pair):
            try:
                with Timeout(15):
                    title, publish_time, content = parser(raw_html)
            except TimeoutError as e:
                print(f"parse error: timeout, at {pkl_fpath}:{i}")
                continue
            except Exception as e:
                print(f"parse error: {e}, at {pkl_fpath}:{i}")
                continue
            else:
                text_id = str(idx).zfill(8)  # generate text id with left2right padding
                time = time if publish_time == "" else publish_time  # time used time format
                if time != "NULL":
                    try:
                        time = parse_datetime(time)
                        print(f"updated time from web page: {time}")
                    except:
                        pass
                try:
                    year, month, day = time.split('-')
                    datetime.datetime(int(year), int(month), int(day))
                except ValueError:
                    time = "NULL"
                else:
                    if int(year) < 1985 or int(year) > 2022:
                        time = "NULL"

                words = calc_word_nums(content)  # calculate word nums according to `\s` nums
                variety = CC  # country code
                genre = "G"  # stand for "general"
                domain = parse_domain(url)  # parse domain for each url
                url = url.strip()
                title, content = preprocess(title), preprocess(content)  # preprocess title and content
                idx += 1

                # drop when word nums less than 5
                if words < 5:
                    continue

                # write new line to output file
                ouput_file.write(f"{text_id}\t{time}\t{words}\t{variety}\t{genre}\t{domain}\t{url}\t{title}\t{content}\n")

    ouput_file.close()
