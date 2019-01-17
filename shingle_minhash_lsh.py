import csv
import argparse
import logging
import re
import glob
import binascii
import itertools
import random
import codecs
import sys
import numpy as np
from operator import itemgetter

max_size = 456976 # 26^4 = 456976 size for the buckets, use 175000 for word-based shingling
hash_number  = 200 # number of hash functions used for minhash
document_number = 427999 # number of songs - 1
prime_number = 438887 # a big prime that is used in minhash function, for character-based use 438887 for word-based use 172987
signature = [[sys.maxsize] * document_number for j in range(hash_number)] # sys.maxsize returns the highest integer value for that particular machine
lsh_number_of_bands = 40 # number of bands
word_per_shingle = 1 # number of words for word-based shingles
lsh_bands = [[0] * document_number for j in range(lsh_number_of_bands)] # lsh band matrix initilization
top_artist_count = 20 # number of artist displayed
stop_array = ['the', 'i', 'you', 'to', 'and', 'a', 'me', 'my', 'in', 'it', 'of', 'your', 'that', 'on', 'im', 'is', 'all', 'for', 'be', 'we', 'dont', 'so', 'with', 'its', 'but', 'this', 'just', 'up', 'no', 'what', 'when', 'can', 'got', 'get', 'do', 'now', 'out', 'oh', 'if', 'youre', 'are', 'never', 'go', 'down', 'was', 'one', 'will', 'they', 'not', 'cant', 'from', 'have', 'she', 'at', 'back', 'as', 'cause', 'her', 'how', 'could', 'aint', 'been', 'there', 'ive', 'where', 'by', 'our', 'more', 'thats', 'or', 'about', 'only', 'through', 'then', 'still', 'thats', 'wont', 'who', 'little', 'every', 'them', 'would', 'had', 'around', 'his', 'around', 'had', 'would', 'always', 'look', 'an', 'ever', 'long', 'off', 'into', 'things', 'these', 'gotta', 'than', 'ya', 'everthing', 'something', 'much', 'before', 'youll', 'gone', 'even', 'him', 'youve', 'did', 'thing', 'going', 'their', 'lets', 'has', 'should', 'same', 'without', 'while', 'whats', 'far', 'hes', 'ohh', 'enough', 'those', 'other', 'might', 'didnt', 'someone', 'because', 'na', 'must', 'yo', 'till', 'many', 'yes', 'everybody', 'yourself', 'theyre', 'nobody', 'any', 'after', 'behind', 'bout', 'under', 'may', 'alright', 'anything', 'sometimes']
# This list stores words that are mostly used in lyrics, we created a program that trace whole lyric list and stores how many count a word is occured. We are removing them from similarity calculation due to the fact that they can cause true negative results
# Char-based kullanırken neden stop array kullanıyoruz

# Character-based

def shing(tup, n):
    shingle_set = set()
    for i in range(len(tup) - n + 1):
        shingle = ""
        shingle = tup[i : i + n]
        hash_shingle = str(shingle)
        p = re.compile(r"^[A-Za-z0-9_.]+$")
        if p.match(hash_shingle):
            shingle_set.add(hash(shingle) % max_size)
    return shingle_set
'''
# Word-based
def shing(tup,n):
    shingle_set = set()
    words = tup.split()
    p = re.compile(r"^[A-Za-z0-9_.]+$")
    for i in range(len(words) - word_per_shingle + 1):
        shingle = ""
        for j in range(i,i + word_per_shingle):
            shingle += words[j]
        hshingle = str(shingle)
        if p.match(hshingle):
            if shingle not in stop_array:
                shingle_set.add(hash(shingle) % max_size)
    return shingle_set
'''
def minhash(shingle_set, permutation_array, k, signature):
    for i in shingle_set:
        for j in range(hash_number):
            if signature[j][k] > permutation_array[i][j]:
               signature[j][k] = permutation_array[i][j]
    
def permutation_array():
    permut = [0] * (hash_number * 2)
    for i in range(hash_number):
        permut[i] = random.randint(1, hash_number)
        permut[2 * i] = random.randint(1, hash_number)
    hash_result_array = [[0] * hash_number for j in range(max_size)]
    for i in range(max_size):
        for j in range(hash_number):
            hash_result_array[i][j] = ((permut[j] * i) + permut[j * 2]) % prime_number
    return hash_result_array

def hash_band(row1, row2, row3, row4, row5):
    return ((row1 + row2 + row3 + row4 + row5) % prime_number)

def compute_lsh(band_number, number_of_rows_in_each_band, signature, lsh_bands):
    i = 0
    j = 0
    curent_band  = 0
    while i < hash_number:  
        while j < document_number:
            lsh_bands[curent_band][j] = hash_band(signature[i][j], signature[i + 1][j], signature[i + 2][j], signature[i + 3][j], signature[i + 4][j])
            j =  j + 1
        i = i + number_of_rows_in_each_band
        j = 0
        curent_band = curent_band + 1

#L2-norm & r = 2
def euclidean_distance(x, y, r = 2.0):
    return sum(((x[i] - y[i]) ** r) for i in range(len(x))) ** (1.0 / r)
    
def cosine_distance(x, y):
    prodAB = sum([(x[i] * y[i]) for i in range(len(x))])
    zeros = [0] * len(x)
    A = euclidean_distance(x, zeros)
    B = euclidean_distance(y, zeros)
    return prodAB / (A * B)

def jaccard_distance_similarity(signature, jaccard_matrix):
    identicalCandidatePairs = 0
    similar = [[0.0] * document_number for j in range(document_number)]
    for i in range(document_number):
        for j in range(document_number):
            for k in range(hash_number):
                if signature[k][i] == signature[k][j] and i != j:
                    identicalCandidatePairs = identicalCandidatePairs + 1
                if signature[k][i] == signature[k][j]:
                     jaccard_matrix[i][j] = identicalCandidatePairs
            identicalCandidatePairs = 0
        
def compute_similarity(t, artist_similar_matrix, artist_list, distinct_artists_list, artist_similar_matrix_count, signature):
    identicalCandidatePairs = 0
    numpy_signature = np.asarray(signature)
    candidates = {}
    for i in range(lsh_number_of_bands):
        buckets = [[] for j in range(prime_number)]
        for j in range(document_number):
            buckets[lsh_bands[i][j]].append(j)
        for item in buckets:
            if len(item) > 1:
                pair = (item[0], item[1])
                if pair not in candidates:
                    A = numpy_signature[:, item[0]]
                    B = numpy_signature[:, item[1]]
                    similarity = cosine_distance(A,B)
                    if similarity >= t:
                        candidates[pair] = similarity
                        artis_name_one = artist_list[item[0]]
                        artis_name_two = artist_list[item[1]]
                        artist_similar_matrix[distinct_artists_list.index(artis_name_one)][distinct_artists_list.index(artis_name_two)] = artist_similar_matrix[distinct_artists_list.index(artis_name_one)][distinct_artists_list.index(artis_name_two)] + similarity
                        artist_similar_matrix_count[distinct_artists_list.index(artis_name_one)][distinct_artists_list.index(artis_name_two)] = artist_similar_matrix_count[distinct_artists_list.index(artis_name_one)][distinct_artists_list.index(artis_name_two)] + 1
                    identicalCandidatePairs = 0
        print("Bucket number " + str(i))
        for bucket_in in buckets:
            print(str(bucket_in))
    sort = sorted(candidates.items(), key=itemgetter(1), reverse=True)
    return candidates,sort

def find_most_similar_artists(artist_similar_matrix,artist_count, distinct_artists_list, artist_similar_matrix_count):
    temp = 0
    name1 = ''
    name2 = ''
    did = 0
    top = [[0.0] * 3 for j in range(20)]
    f= open("similarity.txt","w+")
    for i in range(artist_count):
        for j in range(artist_count):
            for m in range(20):
                if (artist_similar_matrix[i][j] / artist_similar_matrix_count[i][j] ) > top[m][2] and id(distinct_artists_list[i]) != id(distinct_artists_list[j]) and did != 1:
                    top[m][2] = artist_similar_matrix[i][j] / artist_similar_matrix_count[i][j]
                    top[m][1] = i
                    top[m][0] = j
                    did = 1
            did = 0
    for i in range(20):
        print('Most similar artists are ', distinct_artists_list[top[i][0]], distinct_artists_list[top[i][1]], top[i][2])
        f.write('Most similar artists are ' + str(distinct_artists_list[top[i][0]]) + ' ' + str(distinct_artists_list[top[i][1]]) + ' ' + str(top[i][2]) + "%\n")
    f.close()


with codecs.open("metro_lyrics_stop_word.csv", "r", encoding='utf-8', errors='ignore') as csvfile:
    reader = csv.DictReader(csvfile)
    i = 0
    n = 4
    hash_result_array = permutation_array()
    artist_list = ['' for j in range(document_number)]
    artist_set = set()
    print('All the required preparations has been completed!')

    for row in reader:
        if row['text'] is not None:
            artist_list[i] = row['artist']
            artist_set.add(row['artist'])
            shingle_set = shing(row['text'].lower(), n)
            minhash(shingle_set, hash_result_array, i, signature)
        i = i + 1
    print('Shingling and Minhashing has finished!')

    compute_lsh(0, 5, signature, lsh_bands)
    print('LSH is Finished')
    artist_similar_matrix = [[0] * len(artist_set) for j in range(len(artist_set))]
    artist_similar_matrix_count = [[1] * len(artist_set) for j in range(len(artist_set))]
    distinct_artists_list = list(artist_set)
    candidates,sort = compute_similarity(0.3, artist_similar_matrix, artist_list, distinct_artists_list, artist_similar_matrix_count, signature)
    print('Similarity has been completed!')

    find_most_similar_artists(artist_similar_matrix, len(artist_set), distinct_artists_list, artist_similar_matrix_count)
