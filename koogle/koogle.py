# all the imports
import re
import os
import sqlite3
from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash
from DBManager import Index, get_html_text

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

    files = index_engine.get_inactive_files()
    files = [{'title': title, 'id': id} for title, link, preview, id in files]

    print(files)

    return render_template('index.html', words=blacklist, files=files)

@app.route('/search', methods=['POST'])
def search():
    query = request.form['search']
    files = index_engine.search(query)
    words = index_engine.get_search_words(query)

    if files is None:
        return render_template('search_results.html', query=query)

    documents = list()

    for file_id in files:
        title, link, text, active = index_engine.get_file_info(file_id)        
        
        if active == 1:
            text = ' '.join(re.sub("[^\w]", " ",  text).split())
            documents.append({'name': title, 'content': text, 'link': link, 'id': file_id})
    
    return render_template('search_results.html',
                           query=query,
                           documents=documents,
                           words=['as', 'in'])


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

@app.route('/remove_file', methods=['POST'])
def remove_file():
    id = request.form['btn']
    index_engine.remove_file(id)
    return redirect(url_for('home'))


@app.route('/reactivate_file', methods=['POST'])
def reactivate_file():
    id = request.form['btn']
    index_engine.activate_file(id)
    return redirect(url_for('home'))