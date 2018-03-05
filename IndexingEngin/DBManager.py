#!/usr/bin/env python3

import sqlite3
import os
import re
import argparse
import pprint
import gzip
import collections
import itertools

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SPLIT_PATTERN = '|'.join(['\t', '\n', ' ', ',', '\\.', ';', ':', '"', '\\\\', '!', '\\?', '_'])
FS_BUCKET_SIZE = 1000
with open(os.path.join(SCRIPT_DIR, 'stoplist.txt'), 'r') as f:
    STOP_LIST = f.read().split()


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


class Index:
    def __init__(self, db_dir):
        # Create the index directory if it does not already exist
        if not os.path.exists(db_dir):
            os.mkdir(db_dir)
            os.mkdir(os.path.join(db_dir, 'docs'))

        # Open / create the database
        self.base = db_dir
        self.db = sqlite3.connect(os.path.join(db_dir, 'index.sqlite3'))
        self.db.create_function('levenshtein_distance', 2, levenshtein_distance)

        # Create the DB tables if missing
        if not table_exists(self.db, 'terms'):
            self.db.execute('''CREATE TABLE terms (term     TEXT NOT NULL);''')
            # Create an index into the terms table in order to enable faster lookup
            self.db.execute('''CREATE UNIQUE INDEX term_index ON terms(term);''')

        if not table_exists(self.db, 'documents'):
            self.db.execute('''CREATE TABLE documents (title    TEXT NOT NULL, 
                                                           author   TEXT);''')

    def insert_documents(self, documents):
        total_terms = []

        # Insert the new document's metadata into the DB and get its assigned ID
        for document in documents:
            title, author, contents = document

            doc_id = self.db.execute('''INSERT INTO documents (title, author) VALUES (?, ?)''', (title, author)).lastrowid
            doc_path = self.doc_path(doc_id)
            print(doc_id, doc_path)

            with gzip.GzipFile(doc_path, mode='w+') as doc:
                doc.write(bytes(contents, 'UTF-8'))

            terms = re.split(SPLIT_PATTERN, contents) + \
                    re.split(SPLIT_PATTERN, author) + \
                    re.split(SPLIT_PATTERN, title)
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

    def doc_path(self, doc_id):
        """
        Return the path of the document by its ID.

        Documents are separated into "buckets" in order to ensure fast file-system lookup.
        """
        # Partition the documents into directories in order to have faster FS lookup
        bucket = '%04u' % (doc_id / FS_BUCKET_SIZE)
        doc_name = '%04u' % (doc_id % FS_BUCKET_SIZE)

        dir_path = os.path.join(self.base, 'docs', bucket)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        return os.path.join(dir_path, doc_name + '.gz')

    def get_term_id(self, term):
        """ Return the ID of a term, or `None` if it does not exist. """

        q = list(self.db.execute('SELECT rowid FROM terms WHERE term = ?;', (term,)))
        q = list(self.db.execute('SELECT rowid FROM terms WHERE term = ?;', (term,)))
        if len(q) > 0:
            return q[0][0]
        return None

    def get_or_create_term(self, term):
        """ Return the ID of a term; creating it if necessary. """
        term_id = self.get_term_id(term)
        if term_id is not None:
            return term_id

        term_id = self.db.execute('INSERT INTO terms(term) VALUES (?) ', (term,)).lastrowid
        postings_table = 'term_%d' % term_id

        self.db.execute('''CREATE TABLE %s (document_id     INT NOT NULL,
                                                hits            INT NOT NULL,
                                            FOREIGN KEY (document_id) REFERENCES documents(rowid));''' % postings_table)

        return term_id

    def search_term_naive(self, term):
        term_id = self.get_term_id(term)
        postings_table = 'term_%d' % term_id
        return self.db.execute('''
            SELECT      document_id, hits, title, author
            FROM        {tbl}
            INNER JOIN documents ON documents.rowid = {tbl}.document_id
            ORDER BY    hits DESC;
        '''.format(tbl=postings_table))

    def search_query(self, query):
        raise NotImplementedError()

    def similar_terms(self, term):
        return self.db.execute('''
            SELECT * FROM terms WHERE levenshtein_distance(?, term) < 3;
        ''', (term, ))

    def counts(self):
        term_count = next(self.db.execute('SELECT COUNT(*) FROM terms'))[0]
        docs_count = next(self.db.execute('SELECT COUNT(*) FROM documents'))[0]
        total_relations = 0

        relations = {}

        c = self.db.execute('SELECT rowid, term FROM terms')
        for term in c:
            relations[term] = next(self.db.execute('SELECT COUNT(*) FROM term_%u' % term[0]))[0]
            total_relations += relations[term]

        return {
            'terms': term_count,
            'docs': docs_count,
            'relations': total_relations
        }



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

    for doc_path in paths:
        author, title = os.path.basename(doc_path).split('_')
        with open(doc_path, 'r') as doc:
            contents = doc.read()

        docs.append((title, author, contents))

    return docs


def main():
    args = get_args()
    db_location = args.db_location or os.path.expandvars('$HOME/.dana_inverted_index/')
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
        results = list(db.search_term_naive(args.term))
        for res in results:
            print('#%03u "%s" / %s (%u hits)' % (res[0], res[2], res[3], res[1]))
    elif args.subcmd == 'stats':
        pprint.pprint(db.counts())


if __name__ == '__main__':
    main()