import requests
from bs4 import BeautifulSoup
from termcolor import colored #Colored printing for bug test
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['webscale']
col = db['webscale']

def crawler(letter, page, max_page):
    while page <= max_page:
        url = 'http://www.metrolyrics.com/artists-' + letter + '-' + str(page) + '.html'
        source_code = requests.get(url)
        html_text = source_code.text
        b_soup = BeautifulSoup(html_text, 'html.parser')
        for link_artist in b_soup.find_all(itemtype = 'http://schema.org/MusicGroup'):
            artist = link_artist.td.a.get('href')
            try:
                artist_name = str(link_artist.td.a.text[:-7]) #Remove last 7 digits which are ' Lyrics'
                artist_name = artist_name.replace('\n', '')
                #print(colored(artist_name, 'blue'))
                url_artist = str(artist)
                source_code_artist = requests.get(url_artist)
                html_text_artist = source_code_artist.text
                b_soup_artist = BeautifulSoup(html_text_artist, 'html.parser')
                song_table = b_soup_artist.find(class_ = 'songs-table compact')
                try:
                    for link_song in song_table.find_all(class_ = ['title', 'title hasvidtable']):
                        song = link_song.get('href')
                        try:
                            song_name = str(link_song.text[:-7]) #Remove last 7 digits which are ' Lyrics'
                            song_name = song_name.replace('\n', '')
                            #print(colored(song_name, 'green'))
                            url_song = str(song)
                            source_code_song = requests.get(url_song)
                            html_text_song = source_code_song.text
                            b_soup_song = BeautifulSoup(html_text_song, 'html.parser')
                            try:
                                song_text = ''.join([song_info.text for song_info in b_soup_song.find_all(class_ = 'verse')])
                                song_text.decode('utf-8')
                                if len(song_text) >= 20:
                                    song_text = song_text.replace('\n', ' ')
                                    song_text = song_text.replace('\\', '')
                                    #print(song_text)
                                    lyrics = {'artist' : artist_name , 'song' : song_name, 'text' : song_text}
                                    global col
                                    col.insert_one(lyrics)
                            except UnicodeEncodeError:
                                pass#print(colored('UTF-8 Unicode Error Song Text', 'red'))
                        except UnicodeEncodeError:
                            pass#print(colored('UTF-8 Unicode Error Song Name', 'red'))
                except AttributeError:
                    pass#print('No lyrics available')
            except UnicodeEncodeError:
                pass#print(colored('UTF-8 Unicode Error Artist Name', 'red'))
        page = page + 1

crawler('a', 1, 1)
