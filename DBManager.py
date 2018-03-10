#!/usr/bin/env python3

import sqlite3
import os
import re
import argparse
import pprint
import gzip
import collections
import itertools
from logging import error, info
import requests
import bs4

SPLIT_PATTERN = '|'.join(['\t', '\n', ' ', ',', '\\.', ';', ':', '"', '\\\\', '!', '\\?', '_'])

operations = {
    'not': (lambda first, second: 
            [word for word in first if word not in second]),
    'or': (lambda first, second: first + list(set(second) - set(first))),
    'and': (lambda first, second: [word for word in first if word in second])
}

word_re = re.compile("^([a-zA-Z1-9]+)$")
explicit_word_re = re.compile("^\"([a-zA-Z1-9]+)\"$")
complex_query_re = re.compile("([a-zA-Z1-9\"]+|\(.+\))\s+(" + '|'.join(operations.keys()) + ")\s+([a-zA-Z1-9\"]+|\(.+\))")
expr_pattern = "\(.+\)"


def table_exists(cursor, table_name):
    matches = cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    return len(list(matches)) > 0


def levenshtein_distance(s1, s2):
    if len(s1) > len(s2):
        s1, s2 = s2, s1

    distances = range(len(s1) + 1)
    for i2, c2 in enumerate(s2):
        distances_ = [i2+1]
        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1])
            else:
                distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
        distances = distances_
    return distances[-1]


def get_html_text(link):
    # get the file and scrape it from html tags
    file_data = requests.get(link, allow_redirects=True)
    soup = bs4.BeautifulSoup(file_data.content, 'lxml')
    header = soup.find('title').text.strip()

    [s.extract() for s in soup(['style', 'script', '[document]', 'head', 'title'])]
    visible_text = soup.getText()

    info = (header.encode('ascii', 'ignore'), visible_text.encode('ascii', 'ignore'))

    file_data.close()
    return info


class Index:
    def __init__(self, db, temp_dir):
        # Create the index directory if it does not already exist
        if not os.path.exists(temp_dir):
            os.mkdir(temp_dir)
        
        if not os.path.isfile(db):
            open(db, "w+")

        # Open / create the database
        self.db = sqlite3.connect(db)
        self.db.text_factory = str
        self.db.create_function('levenshtein_distance', 2, levenshtein_distance)

        # Create the DB tables if missing
        if not table_exists(self.db, 'terms'):
            self.db.execute("CREATE TABLE terms (term TEXT NOT NULL);")
            # Create an index into the terms table in order to enable faster lookup
            self.db.execute("CREATE UNIQUE INDEX term_index ON terms(term);")

        if not table_exists(self.db, 'documents'):
            self.db.execute("CREATE TABLE documents (title TEXT NOT NULL, link TEXT NOT NULL, preview TEXT, active INT );")

        if not table_exists(self.db, 'blacklist'):
            self.db.execute("CREATE TABLE blacklist (word TEXT NOT NULL);")

    def insert_documents(self, links):
        total_terms = []

        # Insert the new document's metadata into the DB and get its assigned ID
        for link in links:
            info("indexing:" + link)

            # get the file and scrape it from html tags
            title, content = get_html_text(link)

            # insert to the db the author and the title -> it's index  exmp: 0
            doc_id = self.db.execute('''INSERT INTO documents (title, link, preview, active) VALUES (?, ?, ?, ?)''', (title, link, content[:1000], 1)).lastrowid
            
            terms = re.split(SPLIT_PATTERN, content) + re.split(SPLIT_PATTERN, title)
            
            terms = list(map(str.lower, terms))
            
            terms.sort()

            # Aggregate duplicates and append them to the total list.
            # Notice how we ignore short terms (single character)
            total_terms += [(term, doc_id, terms.count(term)) for term in set(terms) if len(term) > 1]

        # Switch to sorting by term instead of by document
        total_terms.sort()

        for term, mapping in itertools.groupby(total_terms, lambda entry: entry[0]):
            for _, doc_id, hits in mapping:
                self.create_or_update_term(term, doc_id, hits)

        self.db.commit()

    def create_or_update_term(self, term, doc_id, hits):
        """ Add a new document connection to the specified term, creating the term in needed. """
        term_id = self.get_or_create_term(term)
        postings_table = 'term_%d' % term_id

        self.db.execute('''INSERT INTO %s(document_id, hits) VALUES(?, ?)''' % postings_table, (doc_id, hits))

    def get_term_table(self, term):
        id = self.get_term_id(term)
        if id is None:
            error("no term id for term")
            return None

        table_name = 'term_%d' % id
        
        if not table_exists(self.db, table_name):
            error("no table exists for term")
            return None
        
        return table_name

    def get_term_id(self, term):
        """ Return the ID of a term, or `None` if it does not exist. """
        query = self.db.execute('SELECT rowid FROM terms WHERE term = ?;', (term,))
        ids = query.fetchall()

        if len(ids) == 0:
            return None

        return ids[0][0]
        
    def get_or_create_term(self, term):
        """ Return the ID of a term; creating it if necessary. """
        term_id = self.get_term_id(term)
        if term_id is not None:
            return term_id

        term_id = self.db.execute('INSERT INTO terms(term) VALUES (?) ', (term,)).lastrowid
        postings_table = 'term_%d' % term_id

        info("creating table for term " + term)

        self.db.execute('''CREATE TABLE %s (document_id     INT NOT NULL,
                                                hits            INT NOT NULL,
                                            FOREIGN KEY (document_id) REFERENCES documents(rowid));''' % postings_table)

        return term_id

    def search_term(self, term):
        term_table = self.get_term_table(term.lower())
        if term_table is None:
            error("no term table for term")
            return None

        query = self.db.execute("SELECT document_id FROM {tbl} ORDER BY hits DESC".format(tbl=term_table))
        return [id[0] for id in query.fetchall()]
        
    def search(self, query):
        query = query.strip()
        print(query)

        if explicit_word_re.match(query):
            word = explicit_word_re.findall(query)[0]
            return self.search_term(word)

        # check if the query is a single word query exmp : "word"
        if word_re.match(query):
            word = word_re.findall(query)[0]
            if self.is_word_in_blacklist(word):
                return list()
            return self.search_term(word)
            
        if complex_query_re.match(query):
            first_exp, operation, second_exp = complex_query_re.findall(query)[0]
            
            if re.match(expr_pattern, first_exp):
                first_lst = self.search(first_exp[1:-1])
            else:
                first_lst = self.search(first_exp)

            if re.match(expr_pattern, second_exp):
                second_lst = self.search(second_exp[1:-1])
            else:
                second_lst = self.search(second_exp)

            if type(first_lst) is list and operation in operations and type(second_lst) is list:
                return operations[operation](first_lst, second_lst)
            else:
                return None

    def get_search_words(self, query):
        query = query.strip()
        print(query)

        if explicit_word_re.match(query):
            word = explicit_word_re.findall(query)[0]
            return [word]

        # check if the query is a single word query exmp : "word"
        if word_re.match(query):
            word = word_re.findall(query)[0]
            if self.is_word_in_blacklist(word):
                return list()
            return [word]
            
        if complex_query_re.match(query):
            first_exp, operation, second_exp = complex_query_re.findall(query)[0]
            
            if re.match(expr_pattern, first_exp):
                first_lst = self.search(first_exp[1:-1])
            else:
                first_lst = self.search(first_exp)

            if re.match(expr_pattern, second_exp):
                second_lst = self.search(second_exp[1:-1])
            else:
                second_lst = self.search(second_exp)

            if type(first_lst) is list and type(second_lst) is list:
                return first_lst + second_lst
            else:
                return list()

    def similar_terms(self, term):
        return self.db.execute('''
            SELECT * FROM terms WHERE levenshtein_distance(?, term) < 3;
        ''', (term, ))

    def get_inactive_files(self):
        return self.db.execute("select title, link, preview, rowid from documents where active == 0").fetchall()

    def get_files(self):
        return self.db.execute("select title, link, active preview from documents").fetchall()

    def get_file_info(self, file_id):
        query = self.db.execute("select title, link, preview, active from documents where rowid = ?", str(file_id))
        file_list = query.fetchall()
        if len(file_list) == 0:
            error("no files found")
            return None

        return file_list[0]
        
    def is_word_in_blacklist(self, word):
        query = self.db.execute('SELECT rowid FROM blacklist WHERE word = ?;', (word,))
        ids = query.fetchall()

        if len(ids) == 0:
            return False

        return True


    def get_blacklist(self):
        return [word[0] for word in self.db.execute("select word from blacklist").fetchall()]

    def add_word_to_blacklist(self, word):
        if not self.is_word_in_blacklist(word):
            self.db.execute("insert into blacklist(word) values (?)", (word,))
            self.db.commit()

    def activate_file(self, doc_id):
        self.db.execute("update documents set active = 1 where rowid = ?", (doc_id,))

    def remove_file(self, doc_id):
        self.db.execute("update documents set active = 0 where rowid = ?", (doc_id,))


#################################################
#                     CLI                       #
#################################################

def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-C', '--db-location', default=None)

    sub_parsers = arg_parser.add_subparsers(dest='subcmd')
    sub_parsers.required = True

    clear_parser = sub_parsers.add_parser('clear')
    stats_parser = sub_parsers.add_parser('stats')
    debug_parser = sub_parsers.add_parser('debug')

    index_parser = sub_parsers.add_parser('index')
    index_parser.add_argument('document_path')
    index_parser.add_argument('--title')
    index_parser.add_argument('--author')

    search_parser = sub_parsers.add_parser('search')
    search_parser.add_argument('term')

    return arg_parser.parse_args()


def get_documents(paths):
    docs = []

    for get_doc_link in paths:
        author, title = os.path.basename(get_doc_link).split('_')
        with open(get_doc_link, 'r') as doc:
            contents = doc.read()

        docs.append((title, author, contents))

    return docs


def main():
    args = get_args()
    db_location = args.db_location or os.path.expandvars('db')
    db = Index(db_location)

    if args.subcmd == 'clear':
        raise NotImplementedError()
    elif args.subcmd == 'index':
        if os.path.isdir(args.document_path):
            paths = [os.path.join(args.document_path, name) for name in os.listdir(args.document_path)]
        else:
            paths = [args.document_path]

        db.insert_documents(get_documents(paths))
    elif args.subcmd == 'search':
        results = list(db.search_term(args.term))
        for res in results:
            print('#%03u "%s" / %s (%u hits)' % (res[0], res[2], res[3], res[1]))
    elif args.subcmd == 'stats':
        pprint.pprint(db.counts())


if __name__ == '__main__':
    main()