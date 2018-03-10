# all the imports
import gzip
import os
import sqlite3
from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash
from DBManager import Index, get_html_text
from wtforms import *
from forms import SearchForm

app = Flask(__name__) # create the application instance :)
app.config.from_object(__name__) # load config from this file , flaskr.py

index_engine = Index('koogle.db', 'db')

# Load default config and override config from an environment variable
app.config.update(dict(
    SECRET_KEY='development key',
    USERNAME='admin',
    PASSWORD='default'
))

app.config.from_envvar('KOOGLE_SETTINGS', silent=True)


# show the home index
@app.route('/')
def show_entries():
    return redirect(url_for('home'))

@app.route('/home')
def home():
    if not session.get('logged_in'):
        return render_template('index.html')
    
    blacklist = index_engine.get_blacklist()
    print(blacklist)

    files = index_engine.get_files()
    files = [{'title': title} for title, link in files]

    print(files)

    return render_template('index.html', words=blacklist, files=files)

@app.route('/search', methods=['POST'])
def search():
    query = request.form['search']
    files = index_engine.search(query)

    if files is None:
        return render_template('search_results.html', query=query)

    documents = list()

    for file_id in files:
        title, link = index_engine.get_file_info(file_id)

        _, text = get_html_text(link)
        
        
        if len(text) > 1000:
            text = text[:1000] + '...'

        documents.append({'name': title, 'content': text, 'link': link})
    
    return render_template('search_results.html',
                           query=query,
                           documents=documents)


@app.route('/add_entry', methods=['POST'])
def add_entry():
    if not session.get('logged_in'):
        abort(401)

    docs = [request.form['link']]

    index_engine.insert_documents(docs)

    return redirect(url_for('home'))


@app.route('/add_word', methods=['POST'])
def add_word():
    if not session.get('logged_in'):
        abort(401)

    print("ds;lgfksd;lgfksl;agfdk;lkg")

    word = request.form['word']

    index_engine.add_word_to_blacklist(word)

    return redirect(url_for('home'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] != app.config['USERNAME']:
            error = 'Invalid username'
        elif request.form['password'] != app.config['PASSWORD']:
            error = 'Invalid password'
        else:
            session['logged_in'] = True
            flash('You were logged in')
            return redirect(url_for('show_entries'))
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    flash('You were logged out')
    return redirect(url_for('show_entries'))



