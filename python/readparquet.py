import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import nltk
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import numpy as np

import string
import sys
import hashlib
import pyarrow as pa
import os
import glob
import pandas as pd
import zipfile
import io

def read_and_print_parquet(file_path):
    # Read the Parquet file into a Dask DataFrame
    ddf = dd.read_parquet(file_path, schema={"ngrams": pa.list_(pa.binary())})
    
    # Print the first 5 rows of the DataFrame
    #print(ddf.head(5))
    return ddf
    
def md5hash(x):
    #words = r['text'].astype(str) a.empty,
    #if (x.empty):
    #    return []
    myx = str(x)
    if myx is None or myx == "":
        return []
    #print(myx)
    text = myx.translate(str.maketrans('', '', string.punctuation))
        # Tokenize the text
    words = nltk.word_tokenize(text.lower())

    words = [word for word in words if word not in stop_words and word.isalnum() and len(word) > 3]
    #return list(ngrams(word_tokenize(text.lower()),10))
    #print("words")
    #print(text)
    #print(words)
    #print(ngrams(word_tokenize(text.lower()),10))
    #return tuple(ngrams(word_tokenize(words.lower()),10))
    ngramlist = list(ngrams(words,5))
    #hashes = [hashlib.md5(str(ngram).encode()).hexdigest() for ngram in ngramlist]
    hashes = [hashlib.md5(str(ngram).encode()).digest() for ngram in ngramlist]
    #print(hashes)
    #print("length of hashes = " + str(len(hashes)))
    #return tuple(ngrams(words,5))
    return hashes    
    
def validate_match_loop(x):  # Check whether two strings share enough n-grams to be a match with str2
    #print("received string" + str(x)[0:50])
    ng1 = set(x)
    
    #print(list(ng1)[:10])
    #ng2 = set(gram2)
    ng2 = set (md5hash(str2))
    #print(ng1)
    #print(ng2)
    if len(ng1) == 0 or len(ng2) == 0:
        return False, 0, 0
    inter = len(ng1.intersection(ng2))
    score1 = inter/len(ng1)
    score2 = inter/len(ng2)
    is_match = max(score1, score2)>0.5 and min(score1, score2)>0.2  # Here's the criteria I use to determine whether an article is a match
    if is_match:
        print("match found" + str(is_match) + str(score1) + str(score2))
    return is_match, score1, score2


def validate_match_two(x,y):  # Check whether two strings share enough n-grams to be a match with str2
    #print("received string" + str(x)[0:50])
    ng1 = set(x)
    ng2 = set(y)
    #print(ng1)
    #print(ng2)
    if len(ng1) == 0 or len(ng2) == 0:
        return False, 0, 0
    inter = len(ng1.intersection(ng2))
    score1 = inter/len(ng1)
    score2 = inter/len(ng2)
    is_match = max(score1, score2)>0.5 and min(score1, score2)>0.2  # Here's the criteria I use to determine whether an article is a match
    return is_match, score1, score2

# don't make this aTdelayed
# read a csv file and return hashes
def process_myfile(file):
        # check excel or csv
        # check excel or csv
    file_path, file_name = os.path.split(file)
    
    if file.lower().endswith('.xlsx'):
        print("reading excel file")
        ddf = dd.from_pandas(pd.read_excel(file, dtype=nydndtypes), npartitions=1)
    elif file.lower().endswith('.csv'):
        ddf = dd.read_csv(file, blocksize="512MB", dtype=nydndtypes)
    else:
        #raise ValueError("Unsupported file format. Please provide an XLSX or CSV file.")
        print("error in file " + file_name)
        return [[]]

    if set(['Print Article Text','Title']).issubset(ddf.columns):
        print("columns exist")
    else:
        print("columns do not exist, simulate error")
        return [[]]
    #############
    ddfint = ddf.drop(columns=['Title', 'URL', 'Copyright Owner', 'Copyright Registration', 'Author(s)'], axis=1)
    ddfint['ngrams'] = ddfint['Print Article Text'].apply(lambda x: md5hash(x), meta=('ngrams', 'bytes'))
    ddfexport = ddfint.drop(columns=['Print Article Text', 'Registration Date', 'Print Publication Date'], axis=1)
    return ddfexport

# don't make this aTdelayed
# read a csv file and return hashes
def process_zipfile(myzipfile):
    with zipfile.ZipFile(myzipfile, 'r') as zip_ref:
        # Get the CSV file from the zip
        with zip_ref.open('DNP-00000186.CSV') as csv_file:
            # Read the CSV file into a Pandas DataFrame
            df = pd.read_csv(io.BytesIO(csv_file.read()))

# Convert the Pandas DataFrame to a Dask DataFrame
    ddf = dd.from_pandas(df, npartitions=1) # Adjust npartitions as needed
    file_path, file_name = os.path.split(myzipfile)
    
    # if file.lower().endswith('.xlsx'):
    #     print("reading excel file")
    #     ddf = dd.from_pandas(pd.read_excel(file, dtype=nydndtypes), npartitions=1)
    # elif file.lower().endswith('.csv'):
    #     ddf = dd.read_csv(file, blocksize="512MB", dtype=nydndtypes)
    # else:
    #     #raise ValueError("Unsupported file format. Please provide an XLSX or CSV file.")
    #     print("error in file " + file_name)
    #     return [[]]

    if set(['Print Article Text','Title']).issubset(ddf.columns):
        print("columns exist")
    else:
        print("columns do not exist, simulate error")
        return [[]]
    #############
    ddfint = ddf.drop(columns=['Title', 'URL', 'Copyright Owner', 'Copyright Registration', 'Author(s)'], axis=1)
    ddfint['ngrams'] = ddfint['Print Article Text'].apply(lambda x: md5hash(x), meta=('ngrams', 'bytes'))
    ddfexport = ddfint.drop(columns=['Print Article Text', 'Registration Date', 'Print Publication Date'], axis=1)
    return ddfexport

## gets all csv files
def get_all_csv_files(directory):
    return glob.glob(os.path.join(directory, '**', '*.*'), recursive=True)

if __name__ == '__main__':
    
    cluster = LocalCluster(
        n_workers=1,
        processes=True,
        threads_per_worker=2
    )
    stop_words = set(stopwords.words('english'))
    client = Client(cluster)
    # Path to the Parquet file
    parquet_file_path = '/home/ubuntu/mypython/nydnout'
    zip_file = '/home/ubuntu/mypython/oaitest/DNP-00000186.zip'
    
    # path to the file to compare
    nydndtypes = {'Title': str, 'URL': str,  'Copyright Owner': str, 'Copyright Registration': str, 'Registration Date': str, 
                  'Author(s)': str, 'Print Publication Date': str, 'Print Article Text': str}
    
    # Read and print the Parquet file
    dataf = read_and_print_parquet(parquet_file_path)
    
    str2 = f"""The Dakota Wizards lead the NBA Development League Eastern  Division after edging the visiting Colorado 14ers for the second  straight game, 108-106 on Sunday at the Bismarck Civic Center"""
    stop_words = set(stopwords.words('english'))
    
    ### ZIP FILES
    #csvfiles = get_all_csv_files("/home/ubuntu/mypython/nydnsample")
    zip_file = '/home/ubuntu/mypython/oaitest/DNP-00000186.zip'
    ddf = process_zipfile(zip_file)
    try:

        for firstindex, firstrow in ddf.iterrows(): # read from csv, zip or other format
            for index, row in dataf.iterrows():    # read from parquet
                #if index <= 10 : 
                    #print(row['ngrams'])
                myword = row['ngrams']
                myword2 = firstrow['ngrams']
                #print(validate_match_loop(myword), row['mysource'])
                result, score1, score2 = validate_match_two(myword, myword2)
                if (result):
                    print("match found " + str(result) + str(score1) + str(score2) + " in location " + row['mysource'])

    except:
        print("error")
        pass
    ###ZIP FILES
    # chunk_size = 1
    
    # for i in range(0, len(csvfiles), chunk_size):
    #     chunk = csvfiles[i:i + chunk_size]
    #     futures = []
    #     for file in chunk:
    #         future = client.submit(process_myfile, file)
    #         futures.append(future)

    #     print("computing futures")
    #     try:
    #         y = client.gather(futures)
    #         #print(y)
    #         # Sum the tuples in y
    #         for ddf in y:
    #             for firstindex, firstrow in ddf.iterrows(): # read from csv or other format
    #                 for index, row in dataf.iterrows():    # read from parquet
    #                     #if index <= 10 : 
    #                         #print(row['ngrams'])
    #                     myword = row['ngrams']
    #                     myword2 = firstrow['ngrams']
    #                     #print(validate_match_loop(myword), row['mysource'])
    #                     result, score1, score2 = validate_match_two(myword, myword2)
    #                     if (result):
    #                         print("match found " + str(result) + str(score1) + str(score2) + " in location " + row['mysource'])


    #         # Print the summed tuples
    #         print ("one outer loop complete")

    #     except:
    #         print("error")
    #         pass

            #break
    print('Completed indeed!')
    