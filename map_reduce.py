# This code is designed via following an example from apache/beam's github page
# https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py

from __future__ import absolute_import

import argparse
import logging
import re
import itertools
from past.builtins import unicode
import random
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class WordExtractingDoFn(beam.DoFn):
    def __init__(self):
        super(WordExtractingDoFn, self).__init__()
        self.lengths_dist = Metrics.distribution(self.__class__, 'len_dist')
        self.empty_line_counter = Metrics.counter(self.__class__, 'empty_lines')
        self.max_size = 175000
        self.word_per_shingle = 1
    def process(self, tup):
        singer, song, lyrics = tup.split(',')
        shingle_set = set()
        words = lyrics.split()

    for i in range(len(words) - self.word_per_shingle + 1):
        shingle = ""
        for j in range(i,i + self.word_per_shingle):
            shingle = shingle + words[j]
            p = re.compile(r"^[A-Za-z0-9_.]+$")
            hshingle = str(shingle)
            if p.match(hshingle):
                shingle_set.add(hash(shingle) % self.max_size)

    yield (song, list(shingle_set))

def run(argv = None):
    a = []
    b = []
    N = 2**32
    for it in range(100):
        a.append(random.randint(1, int(N/2)))
        b.append(random.randint(1, int(N/2)))
  
    hash_number  = 100 # number of hash functions used for minhash
    document_number = 57650 # 57650 number of songs
    prime_number = 438887 # prime number used 438887 in the minhash funciton # 172987 for word based
    lsh_number_of_bands = 20
    top_artist_count = 10
    
    # Create and set your PipelineOptions.
    options = PipelineOptions(flags = argv)
    # For Cloud execution, set the Cloud Platform project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    input_file_name = 'gs://webscalebucket/datason.txt'
    output_file_name = 'gs://webscalebucket/nearDuplicates.txt'
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'quick-discovery-224711'
    google_cloud_options.job_name = 'finddup'
    google_cloud_options.staging_location = 'gs://webscalebucket/temp'
    google_cloud_options.temp_location = 'gs://webscalebucket/stage'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=options)

    # Read the text file[pattern] into a PCollection.
    lines = p | 'read' >> ReadFromText(input_file_name)

    # Count the occurrences of each word.
    def minhash(shingle_array):
        rows = hash_number / lsh_number_of_bands
        signature = []

        for i in range (100):
            res = 2**33
            for hshingle in shingle_array[1]:
                res = min(res,(a[i] * hshingle + b[i] % prime_number))
                signature.append(res)

        for j in range(lsh_number_of_bands):
            lower = int(j * rows)
            upper = int(j * rows + rows)
            yield (signature[lower : upper], shingle_array[0])

    def map_combinations(x):
        for combination in itertools.combinations(x[1], 2):
            yield (list(combination), 1)

    def calculate_percentage(x):
        sum = 0
        print(' calculate_percentage ', x[0])
        for occurences in x[1]:
            sum = sum + occurences
        percentage = float (sum)  / float(lsh_number_of_bands)
        percentage = percentage * 100
        yield (x[0], percentage)

    counts = (lines
              | 'Extract' >> (beam.ParDo(WordExtractingDoFn()))
              | 'Minhash' >> beam.FlatMap(lambda x: minhash(x))
              | 'Reduce' >> beam.GroupByKey()
              | 'Filter' >> beam.Filter(lambda x: len(list(x[1]))>1)
              | 'Combinations' >> beam.FlatMap(lambda x: map_combinations(x))
              | 'All combinations' >>beam.GroupByKey()
              | 'Percentage' >>beam.FlatMap(lambda x: calculate_percentage(x))
              | 'Finaly' >>beam.GroupByKey())

    counts | 'Write' >> WriteToText(output_file_name)

    result = p.run()
    result.wait_until_finish()

    if (not hasattr(result, 'has_job') or result.has_job):
        empty_lines_filter = MetricsFilter().with_name('empty_lines')
        query_result = result.metrics().query(empty_lines_filter)
        if query_result['counters']:
            empty_lines_counter = query_result['counters'][0]
            logging.info('number of empty lines: %d', empty_lines_counter.committed)

    filter = MetricsFilter().with_name('len_dist')
    query_result = result.metrics().query(filter)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
