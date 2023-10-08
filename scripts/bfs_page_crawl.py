# -*- coding: utf-8 -*-
# @author: YangLiu
# @email: yangliu.real@gmail.com

# Asyncio crawler for Breadth-first-search web page crawling, using asyncio and httpx.
# Working trilogy:
# 1. Input a batch of URLs to generate a domain set(De-dup).
# 2. Iterate each of domain in DFS approach, with specific depth(default 5), collect all the URLs which end with {cc} into a uniform set.
# 3. Crawl corresponding web page content according to each URL in the uniform set, dump them as a dict(url2content) object locally.

import os
import asyncio
from typing import Set
from pprint import pprint

import httpx
from tqdm.asyncio import tqdm
from httpx import AsyncClient
from pebble import ProcessPool
from lxml.html import fromstring


HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
LIMIT = httpx.Limits(max_connections=30)
TRANS = httpx.AsyncHTTPTransport(verify=False, retries=3)


def generate_index_set(cc: str) -> Set[str]:
    index_set = set()
    with open(f"data/metadata.raw.{cc}.tsv", "r") as f:
        lines = f.readlines()
        for line in lines:
            components = line.split('\t')
            url = components[6]
            # if not url.startswith("http") or f".{cc}/" not in url:
            if not url.startswith("http"):
                continue
            index_url = url.split(f".{cc}/")[0] + f".{cc}" if f".{cc}/" in url else url
            index_set.add(index_url)

    return index_set


async def bfs_crawl_concurrent(index: int, index_cout: int, index_url: str, cc: str, max_crawl_depth: int = 5) -> Set[str]:
    """Crawl all the URLs in BFS approach with previously set max crawl depth."""
    global total_url_set
    total_url_set = set()
    url_queue = asyncio.Queue(-1)
    client = AsyncClient(headers=HEADERS, limits=LIMIT, transport=TRANS, max_redirects=5, timeout=10, proxies="socks5://127.0.0.1:7890")

    async def collect_all_urls_in_page(url: str) -> Set[str]:
        global total_url_set
        try:
            res = await client.get(url)
        except:
            if url in total_url_set:
                total_url_set.remove(url)
            return set()
        else:
            if url in total_url_set and (res.status_code != 200 or "text/html" not in res.headers["content-type"]):
                total_url_set.remove(url)
                return set()
            try:
                html = res.content.decode("utf-8")
            except:
                if url in total_url_set:
                    total_url_set.remove(url)
                return set()
        try:
            protocol, suffix = url.split("://")
            prefix = protocol + "://"
            domain = suffix.split('/')[0]
            base_url = prefix + domain
            parsed_doc = fromstring(html, base_url=base_url)
            parsed_doc.make_links_absolute()
        except:
            if url in total_url_set:
                total_url_set.remove(url)
            return set()
        link_list = [tpl[2] for tpl in list(parsed_doc.iterlinks()) if tpl[2] not in total_url_set and tpl[2].startswith("http") and (f".{cc}" in tpl[2] or f"/{cc}/" in tpl[2])]
        for new_url in link_list:  # enqueue new urls
            url_queue.put_nowait(new_url)
        # total_url_set = total_url_set.union(link_list)  # add new urls to total_url_set
        return set(link_list)

    def task_done(_future) -> Set[str]:
        return _future.result()

    try:
        total_url_set.add(index_url)
        await url_queue.put(index_url)
        for depth in range(1, max_crawl_depth + 1):
            queue_size = url_queue.qsize()
            task_list = list()
            # print(f"\033[92m[{cc}][pid:{os.getpid()}][{index}/{index_cout}] URL size in level {depth}: {queue_size}\033[00m")
            # print(f"[{cc}][pid:{os.getpid()}][{index}/{index_cout}] URL size in level {depth}: {queue_size}")
            for _ in range(queue_size):
                url = url_queue.get_nowait()
                total_url_set.add(url)
                task = asyncio.create_task(collect_all_urls_in_page(url))
                # task.add_done_callback(task_done)
                task_list.append(task)
            results = [await f for f in tqdm(asyncio.as_completed(task_list, timeout=600), total=len(task_list), unit=" url", unit_scale=True, desc=f"[{cc}][{os.getpid()}][{index}/{index_cout}][level{depth}]")]
            # for s in results:
            # total_url_set = total_url_set.union(s)
            print(f"Size of total_url_set: {len(total_url_set)}")
            # pprint(total_url_set)
    except:
        return total_url_set

    return total_url_set


def worker(index: int, index_cout: int, index_url: str, cc: str, max_crawl_depth: int) -> Set[str]:
    s = asyncio.run(bfs_crawl_concurrent(index, index_cout, index_url, cc, max_crawl_depth))
    return s


def master(index_set: Set[str], cc: str, max_crawl_depth: int) -> Set[str]:

    global total_set
    total_set = set()

    def checkpoint() -> None:
        global total_set
        with open(f"url.{cc}.tsv", "w") as fp:
            for url in total_set:
                fp.write(f"{url}\n")

    def callback(_future) -> None:
        try:
            global total_set
            result_set = _future.result()
            total_set = total_set.union(result_set)
            print(f"[{cc}] Totally {len(total_set)} URLs to write.")
            checkpoint()
        except TimeoutError as error:
            # print(f"\033[91mFunction took longer than {error.args[1]} seconds\033[00m")
            print(f"Function took longer than {error.args[1]} seconds")
        except Exception as error:
            # print(f"\033[91mFunction raised {error}\033[00m")
            print(f"Function raised {error}")

    # for batch in get_chunks(list(index_set), 64):
    with ProcessPool(max_workers=8, max_tasks=16) as pool:
        for i, index_url in enumerate(index_set, start=1):
            print('*' * 100)
            future = pool.schedule(worker, [i, len(index_set), index_url, cc, max_crawl_depth], timeout=3600)
            future.add_done_callback(callback)
    print(f"All task done for {cc}.")

    return total_set


if __name__ == "__main__":
    # bfs crawl urls for each specific domain
    for cc in ["cn", "hk", "mo", "tw", "sg", "my"]:
        index_set = generate_index_set(cc)
        s = master(index_set, cc, 3)
        with open(f"url.{cc}.tsv", "w") as fp:
            for url in s:
                fp.write(f"{url}\n")
