# -*- coding: utf-8 -*-
# @author: YangLiu
# @email: yangliu.real@gmail.com

# utility tool box

import re
import signal
from time import sleep

from httpx import get, post


class Timeout:
    """Timeout class for timing and avoiding long-time string processing."""

    def __init__(self, seconds=1, error_message="Timeout"):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


def generate_trigram(fpath: str = "data/glowbe.raw.txt"):
    """silly func for generate trigram set."""
    gram_rec = dict()
    pattern = re.compile(r"[^a-zA-Z\s]+", re.IGNORECASE)
    output_lines = list()
    output_file = open("data/3gram.tsv", "w")
    with open(fpath, mode="r", encoding="utf-8") as f:
        lines = f.readlines()
        for i, line in enumerate(lines, start=1):
            if not line.startswith("#"):
                continue
            line = line.split(' ', 1)[1].replace("<p>", "").replace("<h>", "").lower()
            res = re.sub(pattern, "", line)
            res = " ".join([w.strip() for w in res.split() if w.strip() != ""])
            # output_file.write(res + "\n")
            output_lines.append(res)
            # if i > 1000:
            # break

    del lines
    # core in trigram generation
    for line in output_lines:
        l, r = 0, 3
        word_list = line.split()
        if len(word_list) < 3:
            continue
        for i, word in enumerate(word_list):
            if r > len(word_list):
                break
            gram = " ".join(word_list[l:r])
            if gram not in gram_rec:
                gram_rec[gram] = 0
            gram_rec[gram] += 1
            l += 1
            r += 1
    gram_rec = {k: v for k, v in sorted(gram_rec.items(), key=lambda item: item[1])}
    output_file.write("3gram\tfrequency\n")
    size = 100000 if len(gram_rec) > 100000 else len(gram_rec)
    for _ in range(size):
        item = gram_rec.popitem()
        output_file.write(item[0] + "\t" + str(item[1]) + "\n")

    output_file.close()


def req_2captcha(
    url=None,
    sitekey=None,
    data_s=None,
    cookies=None,
    proxy=None
):
    status_code = 0
    url_req = "http://2captcha.com/in.php"
    form = {"method": "userrecaptcha",
            "googlekey": sitekey,
            "key": "4716d26a56bd509f04393c429132f257",
            "pageurl": url,
            "data-s": data_s,
            "cookies": cookies,
            "json": 1
            }
    res = post(url_req, data=form)
    request_id = res.json()["request"]
    url_res = f"http://2captcha.com/res.php?key=4716d26a56bd509f04393c429132f257&action=get&id={request_id}&json=1"
    while not status_code:
        res = get(url_res)
        if res.json()["status"] == 0:
            sleep(3)
        else:
            token = res.json()["request"]
            # print(token)
            return token


def get_chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def cluster_documents() -> None:
    """Input a documents from all 6 varieties, cluster them by their specific top domain."""
    fpath_list = glob("../../data/*.tsv")
    cc2list = {"cn": [], "hk": [], "mo": [], "tw": [], "sg": [], "my": []}
    for fpath in fpath_list:
        cc = fpath.replace(".tsv", "").rsplit('.')[-1]
        cc_list_exclude = ["cn", "hk", "mo", "tw", "sg", "my"]
        _ = cc_list_exclude.pop(cc_list_exclude.index(cc))
        with open(fpath, "r") as f:
            lines = [line.strip() for line in f.readlines()]
            for line in lines:
                _, _, _, _, _, domain, _, _, _ = line.split('\t')
                top_domain = domain.rsplit('.')[-1].strip()
                if top_domain in cc_list_exclude:
                    print(f"{cc} | true: {top_domain}")
                    cc2list[top_domain].append(line)
                else:
                    cc2list[cc].append(line)
    for cc, lines in cc2list.items():
        with open(f"merge.{cc}.tsv", "w") as f:
            for line in lines:
                f.write(line + "\n")


def get_corpus_statistics():
    """
    1. number of word(token) for each variety
    2. number of paragraphs for each variety
    3. number of sentences for each variety
    """
    pass


if __name__ == "__main__":
    # cluster_documents()
    url_freq_rank()
    # req_2captcha(url="https://www.google.com/recaptcha/api2/demo", sitekey="6Le-wvkSAAAAAPBMRTvw0Q4Muexq9bi0DJwx_mJ-")
    # generate_trigram()
