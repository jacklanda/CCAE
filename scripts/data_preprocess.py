#!/usr/bin/python3

# -*- coding: utf-8 -*-
# @author: YangLiu
# @email: yangliu.real@gmail.com

# CCbE Corpus Data Generation
# Generation procedure of from tbl format -> GloWbE-like format.
#
# word-lemma-pos: (Done)
#     use function `pos` to generate PoS data for each input text, every input text
#     share the same TextID(marked with TextID at the head of each article).
# lexicon: (Done)
#     use function `wlp2lexicon` to input all the lines in wlp data, and calculate each item
#     with its frequency in it, and then sort items by frequency, finally output to a lexicon file.
#     n.b. all items should be treated case-sensitively.
# db: (Done)
#     TextID, SequenceWordID, WordID
#     leverage word-lemma-pos and lexicon data jointly.
# sources-fulltext: (Done)
#     combile Sources and FullText to a single file
#     include TextID, Words, Country, Genre, URL, Title, Content, totally 7 columns.


import os
from glob import glob
from os.path import isdir
from typing import List

import spacy

# metadata tsv files' directory
METADATA_DIR = "merge"
SLIDING_WINDOW_SIZE = 21
PIPELINE = spacy.load("en_core_web_sm")


def get_split_line(header: str) -> str:
    """Add length-self-adaptable split line between header & body in tsv."""
    split_line = ""
    components = header.split('\t')
    for component in components:
        split_line = f"{split_line}\t{'-' * len(component)}" if split_line != "" else f"{'-' * len(component)}"
    return f"{split_line}\n"


def generate_wlp_file(cc: str, file_path: str) -> str:
    """Generate word-lemma-pos file.

    Output Format
    -------------
    TextID  SequenceWordID  Word    Lemma   PoS Tag IsStopWord  IsSentenceStart IsSentenceEnd
    """
    wlp_file_path = f"CCbE/{cc}/wlp.tsv"

    with open(file_path, "r") as fr, \
            open(wlp_file_path, "a+") as fw:
        lines = fr.readlines()
        header = f"TextID\tSequenceWordID\tWord\tLemma\tPoS\tTag\tIsStopWord\tIsSentenceStart\tIsSentenceEnd\n"
        # print(header, end="")
        fw.write(header)
        fw.write(get_split_line(header))
        for idx, line in enumerate(lines):
            if idx == 0:
                continue
            textid = line.split('\t')[0]
            full_text = line.split('\t')[-1]
            doc = PIPELINE(full_text)
            for token in doc:
                # SequenceWordID	Word	Lemma	PoS	Tag	IsStopWord	IsSentenceStart	IsSentenceEnd
                new_line = f"{textid}\t{str(token.i).zfill(9)}\t{token.text}\t{token.lemma_}\t{token.pos_}\t{token.tag_}\t{token.is_stop}\t{token.is_sent_start}\t{token.is_sent_end}\n"
                if "SPACE" in new_line and "_SP" in new_line:
                    continue
                # print(new_line, end="")
                fw.write(new_line)

    return wlp_file_path


def generate_lexicon_file(cc: str, wlp_file_path: str) -> str:
    """Generate lexicon file
    Count `Word + Lemma + PoS`'s frequency using dictionary.

    Output Format
    -------------
    WordID  Freq    Word    Lemma   PoS Tag
    """
    wlpt2freq = dict()
    lexicon_file_path = f"CCbE/{cc}/lexicon.tsv"

    with open(wlp_file_path, "r") as fr:
        for idx, line in enumerate(fr.readlines()):
            if idx == 0 or idx == 1:
                continue
            _, _, word, lemma, pos, tag, _, _, _ = line.split('\t')
            wlpt = f"{word}\t{lemma}\t{pos}\t{tag}"
            if wlpt not in wlpt2freq:
                wlpt2freq[wlpt] = 0
            wlpt2freq[wlpt] += 1

    sorted_pairs = sorted(wlpt2freq.items(), key=lambda item: item[1], reverse=True)

    with open(lexicon_file_path, "a+") as fw:
        header = f"WordID\tFreq\tWord\tLemma\tPoS\tTag\n"
        # print(header, end="")
        fw.write(header)
        fw.write(get_split_line(header))
        for idx, pair in enumerate(sorted_pairs, start=1):
            wlpt, freq = pair[0], pair[1]
            word_id = str(idx)
            word, lemma, pos, tag = wlpt.split('\t')
            line = f"{word_id}\t{freq}\t{word}\t{lemma}\t{pos}\t{tag}\n"
            # print(line, end="\n")
            fw.write(line)

    return lexicon_file_path


def generate_db_file(cc: str, wlp_file_path: str, lexicon_file_path: str) -> str:
    """Generate db file.

    Output Format
    -------------
    TextID  SequenceWordID  WordID
    """
    wlpt2wordid = dict()
    db_file_path = f"CCbE/{cc}/db.tsv"

    with open(lexicon_file_path, "r") as fr:
        for line in fr.readlines():
            word_id, _, word, lemma, pos, tag = line.split('\t')
            wlpt = f"{word}\t{lemma}\t{pos}\t{tag}".strip()
            wlpt2wordid[wlpt] = word_id

    with open(wlp_file_path, "r") as fr, \
            open(db_file_path, "a+") as fw:
        header = f"TextID\tSequenceWordID\tWordID\n"
        fw.write(header)
        fw.write(get_split_line(header))
        for idx, line in enumerate(fr.readlines()):
            if idx == 0 or idx == 1:
                continue
            text_id, sequence_word_id, word, lemma, pos, tag, _, _, _ = line.split('\t')
            wlpt = f"{word}\t{lemma}\t{pos}\t{tag}"
            word_id = wlpt2wordid[wlpt] if wlpt in wlpt2wordid else "OOV"
            fw.write(f"{text_id}\t{sequence_word_id}\t{word_id}\n")

    return db_file_path


def generate_sources_file(cc: str, db_file_path: str) -> str:
    """Generate sources file.

    Output Format
    -------------
    TextID  Time    Words   Variety Genre   Domain  URL Title   Content
    """
    sources_file_path = f"CCbE/{cc}/sources.tsv"
    os.system(f"cp {METADATA_DIR}/metadata.raw.{cc}.tsv {sources_file_path}")
    return sources_file_path


# Unuse for now
def split_single_item(item: str) -> List[str]:
    sub_item_list = list()
    idx, date, words, cc, text_type, domain, url, title, content = item.split('\t')
    word_list = content.split(' ')
    content_piece = ""
    for i in range(len(word_list)):
        if i != 0 and i % SLIDING_WINDOW_SIZE == 0:
            # ID  TextID  Time  TextWords  Variety  Genre  Domain  URL  Title  Content  FullTextContent
            # sub_item_list.append(f"{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n")
            content_piece = ""
        content_piece = f"{word_list[i].strip()}" if content_piece == "" else f"{content_piece} {word_list[i].strip()}"

    return sub_item_list


# Unuse for now
def split_corpus(fpath: str):
    """
    If we cannot charge you, you remain responsible for any uncollected amounts.
    w1 w2   w3     w4   w5    w6

    Procedure
    ---------
    1. Split the content of a single post into multiple pieces from head to tail using a fixed-size(21 as default) sliding window.
    ### TODO

    Issues
    ------
    1. How to deal with punctuations like `,`, `.`, `:`, etc.
    """
    new_lines = list()
    with open(fpath, "r") as f:
        items = f.readlines()
        for item in items:
            sub_item_list = split_single_item(item)
            new_lines += sub_item_list

    return new_lines


# Unuse for now
def sort_lines(lines: List[str]) -> List[str]:
    lines_to_write = list()
    for idx, line in enumerate(lines, start=1):
        new_line = f"{str(idx).zfill(8)}\t{line}"
        lines_to_write.append(new_line)

    return lines_to_write


# Unuse for now
def merge2tsv(fpath1: str, fpath2: str) -> List[str]:
    url_set = set()
    lines_to_sort = list()
    with open(fpath1, "r") as file_new, \
            open(fpath2, "r") as file_old:
        lines_new = file_new.readlines()
        lines_old = file_old.readlines()

        for line in lines_new:
            url = line.split('\t')[5]
            content = line.split('\t')[7].split(' ')
            if url in url_set or len(content) < 150:
                continue
            url_set.add(url)
            lines_to_sort.append(line)

        for line in lines_old:
            url = line.split('\t')[5]
            content = line.split('\t')[7].split(' ')
            if url in url_set or len(content) < 150:
                continue
            url_set.add(url)
            lines_to_sort.append(line)

    return lines_to_sort


# Archive code
def archive():
    """
    file_path_lists = glob("new/*tsv")

    id2doc = dict()
    for fpath in file_path_lists:
        # with open(f"{fpath}.output", "a+") as fw:
        with open(fpath, "r") as fr:
            for line in fr.readlines():
                id_, date_, other_ = line.split('\t', maxsplit=2)
                if not id_.isdigit():
                    continue
                if date_ == "NULL":
                    pass
                elif '-' not in date_:
                    year = date_[:4]
                    month = date_[4:6]
                    day = date_[6:8]
                    date_ = f"{year}-{month}-{day}"
                id2doc[int(id_)] = f"{id_}\t{date_}\t{other_}"
        id2doc = dict(sorted(id2doc.items()))
        with open(f"output/{fpath.split('/')[1]}", "a+") as fw:
            for _, v in id2doc.items():
                fw.write(v)
    """
    """
    fpath_list = glob("new/*tsv")
    for fpath in fpath_list:
        fpath_new = fpath
        fpath_old = fpath.replace("new", "old")
        fpath_output = fpath.replace("new", "merge")
        print(fpath_new, fpath_old, fpath_output)
        lines_to_sort = merge2tsv(fpath_new, fpath_old)
        lines_to_write = sort_lines(lines_to_sort)
        with open(fpath_output, "a+") as f:
            # TextID  Time  Words  Variety  Genre  Domain  URL  Title  Content
            f.write(f"TextID\tTime\tWords\tVariety\tGenre\tDomain\tURL\tTitle\tContent\n")
            for line in lines_to_write:
                f.write(line)
    """
    pass


def preprocess(file_path: str):
    cc = file_path.split('.')[-2]
    cc_dir = f"CCbE/{cc}"

    if not os.path.isdir(cc_dir):
        os.mkdir(cc_dir)
    else:
        os.rmdir(cc_dir)

    wlp_file_path = generate_wlp_file(cc, file_path)
    # wlp_file_path = f"CCbE/wlp.mo.tsv"
    print(wlp_file_path, "=> ", end="")

    lexicon_file_path = generate_lexicon_file(cc, wlp_file_path)
    # lexicon_file_path = f"CCbE/lexicon.mo.tsv"
    print(lexicon_file_path, "=> ", end="")

    db_file_path = generate_db_file(cc, wlp_file_path, lexicon_file_path)
    # db_file_path = f"CCbE/db.mo.tsv"
    print(db_file_path, "=> ", end="")

    sources_file_path = generate_sources_file(cc, db_file_path)
    print(sources_file_path)

    print(f"{cc} done.")


if __name__ == "__main__":
    file_path_list = glob(f"{METADATA_DIR}/*.tsv")
    for file_path in file_path_list:
        preprocess(file_path)
    print("All done.")
