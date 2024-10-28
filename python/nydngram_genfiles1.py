from dask.distributed import Client, LocalCluster
import multiprocessing as mp
import numpy as np

from time import sleep
import os
import glob
from dask.distributed import Client, wait
from dask.utils import parse_bytes
from dask import delayed
from dask import dataframe as dd
from dask import compute
from dask import persist
import pandas as pd
import nltk
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import numpy as np
import string
import sys
import hashlib
import pyarrow as pa

#nltk.download('stopwords')

NYDN_CSV_INPUT_DIRECTORY = "/home/vinkai/ari/ARI_search/python/nydn"
NYDN_TRUNCATED_NGRAM_OUTPUT_FILE = '/home/vinkai/ari/ARI_search/python/nydngramsouttrunc'

PJK_CSV_INPUT_DIRECTORY = '/Users/kootsoop/Machina_NYT - Vol 8 Stories/'
PJK_TRUNCATED_NGRAM_OUTPUT_FILE = "NGRAMS.out"

CSV_INPUT_DIRECTORY = NYDN_CSV_INPUT_DIRECTORY
TRUNCATED_NGRAM_OUTPUT_FILE = NYDN_TRUNCATED_NGRAM_OUTPUT_FILE

NYTDropColumns = ['digital_pub_date', 'print_pub_date', 'url', 'headline', 'is_archive_url', 'byline']
NYTArticleColumn = 'text'

NYDNDropColumns = ['Title', 'URL', 'Copyright Owner', 'Copyright Registration', 'Author(s)']
NYDNArticleColumn = 'Print Article Text'

ArticleColumn = NYDNArticleColumn
DropColumns = NYDNDropColumns

column_types = {}
column_types[ArticleColumn] = str

for item in DropColumns:
    column_types[item] =  str

## generates and store n grams
def get_all_csv_files(directory):
    return glob.glob(os.path.join(directory, '**', '*.*'), recursive=True)

@delayed
def myfoo(x):
    if x is None or x == "":
        return ""
    myx = str(x)
    text = myx.translate(str.maketrans('', '', string.punctuation))
    # Tokenize the text
    words = nltk.word_tokenize(text.lower())

    words = [word for word in words if word not in stop_words and word.isalnum() and len(word) > 3]
    ngramlist = list(ngrams(words,5))
    hashes = [hashlib.md5(str(ngram).encode()).digest() for ngram in ngramlist]
    return hashes

def myfoo_nodelay(x):
    myx = str(x)
    if myx is None or myx == "":
        return []
    text = myx.translate(str.maketrans('', '', string.punctuation))
    # Tokenize the text
    words = nltk.word_tokenize(text.lower())

    words = [word for word in words if word not in stop_words and word.isalnum() and len(word) > 3]
    ngramlist = list(ngrams(words,5))
    hashes = [np.uint32(int(hashlib.md5(str(ngram).encode()).hexdigest()[0:8],16)) for ngram in ngramlist]
    return hashes

def tokenize_text(x):
    return x['url'].str.split()

def to_lower(text):
   return np.char.split('the quick brown fox')

@delayed
def validate_match(gram1,str2):  # Check whether two strings share enough n-grams to be a match
    
    ng1 = set(gram1)
    ng2 = set (myfoo_nodelay(str2))
    if len(ng1) == 0 or len(ng2) == 0:
        return False, 0, 0
    inter = len(ng1.intersection(ng2))
    score1 = inter/len(ng1)
    score2 = inter/len(ng2)
    is_match = max(score1, score2)>0.5 and min(score1, score2)>0.2  # Here's the criteria I use to determine whether an article is a match
    return is_match, score1, score2

def validate_match_nodelay(gram1,str2):  # Check whether two strings share enough n-grams to be a match
    
    ng1 = set(gram1)
    ng2 = set (myfoo_nodelay(str2))
    if len(ng1) == 0 or len(ng2) == 0:
        return False, 0, 0
    inter = len(ng1.intersection(ng2))
    score1 = inter/len(ng1)
    score2 = inter/len(ng2)
    is_match = max(score1, score2)>0.5 and min(score1, score2)>0.2  # Here's the criteria I use to determine whether an article is a match
    return is_match, score1, score2

def validate_match_loop(x):  # Check whether two strings share enough n-grams to be a match
    ng1 = set(x)
    ng2 = set (myfoo_nodelay(str2))
    if len(ng1) == 0 or len(ng2) == 0:
        return False, 0, 0
    inter = len(ng1.intersection(ng2))
    score1 = inter/len(ng1)
    score2 = inter/len(ng2)
    is_match = max(score1, score2)>0.5 and min(score1, score2)>0.2  # Here's the criteria I use to determine whether an article is a match
    if is_match:
        print("match found" + str(is_match) + str(score1) + str(score2))
    return is_match, score1, score2

# Define a function to apply to each partition
def add_column(df):
    return( myfoo_nodelay(df [ArticleColumn] ))
    

# don't make this aTdelayed
def process_myfile(file):
    # check excel or csv
    file_path, file_name = os.path.split(file)
    if file.lower().endswith('.xlsx'):
        print("reading excel file")
        ddf = dd.from_pandas(pd.read_excel(file, dtype=column_types), npartitions=1)
    elif file.lower().endswith('.csv'):
        ddf = dd.read_csv(file, blocksize="512MB", dtype=column_types)
    else:
        print("error in file " + file_name)
        return 0,0

    if set([ArticleColumn]).issubset(ddf.columns):
        print("columns exist")
    else:
        print("columns do not exist, simulate error")
        return 0, 0
    #############
    ddfint = ddf.drop(columns=DropColumns, axis=1)
 
    ddfint['ngrams'] = ddfint[ArticleColumn].apply(lambda x: myfoo_nodelay(x), meta=('ngrams', 'uint32'))
    ddfint['mysource'] = ddfint.apply(lambda x: str(x.name) + file_name, axis=1, meta=('mysource', 'str'))
    
    print("xporting to parquet")
    # name_function = lambda x: f"data-{x}.parquet"
    ddfexport = ddfint.drop(columns=[ArticleColumn], axis=1)
    ddfexport.to_parquet(TRUNCATED_NGRAM_OUTPUT_FILE, schema={"ngrams": pa.list_(pa.uint32())}, name_function=lambda x: f"{file_name}data-{x}.parquet")
    return 0, 0

if __name__ == '__main__':
    cluster = LocalCluster(
        n_workers=4,
        processes=True,
        threads_per_worker=2
    )
    stop_words = set(stopwords.words('english'))
    str2 = """Mary GordonBob . . . . . John WarburtonSuco . . . . . Claud AllisterBertie . . . . . Will StantonGovernor's Aide . . . . . Edgar NortonTess Bailey . . . . . Margaret RoachDuffy . . . . . Billy BevanIt was probably inevitable that the first film starring Annabella to be released following her romantic union with Tyrone Power should bear a title such as ""Bridal Suite,"" which opened yesterday at the Capitol. But don't be misled by the label; the characters depicted herein are fictitious and bear absolutely no similarity to actual persons, living or dead.In fact, they bear little resemblance to anything conceivable. For seldom, if ever, have actors been required to sail through the old story of the scapegrace American millionaire and the shy little alpine maid who fall in love with less of a script to convey them. It is downright painful to behold an expert cast of generally fun-loving cut-ups toil and struggle under the load of joyless dialogue and trite situation which is mercilessly heaped upon them.At the CapitolBRIDAL SUITE, screen play by Samuel Hoffenstein based on a story by Gottfried Reinhardt and Virginia Faulkner; directed by William Thiele; produced by Edgar Selwyn for Metro-Goldwyn-Mayer.Luise Anzengruber . . . . . AnnabellaNeil McGill . . . . . Robert YoungDoctor Grauer . . . . . Walter ConnollySir Horace Bragdon . . . . . Reginald OwenCornelius McGill . . . . . Gene LockhartLord Helfer . . . . . Arthur TreacherMrs. McGill . . . . . Billie BurkeAbbie Bragdon . . . . . Virginia FieldMaxl . . . . . Felix Bressart"""
    str1 = """Mary GordonBob . . . . .. . . Billy BevanIt was probably inevitable that the first film starring Annabella to be released following her romantic union with Tyrone Power ""Bridal Suite,"" which opened yesterday at the Capitol. But don't be misled by the label; the characters depicted herein are fictitious and bear absolutely no similarity to actual persons, living or dead.In fact, they bear little resemblance to anything conceivable. For seldom, if ever, have actors been required to sail through the old story of the scapegrace American millionaire and the shy little alpine maid who fall in love with less of a script to convey them. It is downright painful to behold an expert cast of generally fun-loving cut-ups toil and struggle under the load of joyless dialogue and trite situation which is mercilessly heaped upon them.At the CapitolBRIDAL SUITE, screen play by Samuel Hoffenstein based on a story by Gottfried Reinhardt and Virginia Faulkner; directed by William Thiele; produced by Edgar Selwyn for Metro-Goldwyn-Mayer.Luise Anzengruber . . . . . AnnabellaNeil McGill . . . . . Robert YoungDoctor Grauer . . . . . Walter ConnollySir Horace Bragdon . . . . . Reginald OwenCornelius McGill . . . . . Gene LockhartLord Helfer . . . . . Arthur TreacherMrs. McGill . . . . . Billie BurkeAbbie Bragdon . . . . . Virginia FieldMaxl . . . . . Felix Bressart"""
    client = Client(cluster)
    csvfiles = get_all_csv_files(CSV_INPUT_DIRECTORY)
    chunk_size = 4
    for i in range(0, len(csvfiles), chunk_size):
        chunk = csvfiles[i:i + chunk_size]
        zs = [] #dask delayed
        futures = []
        for file in chunk:
            future = client.submit(process_myfile, file)
            futures.append(future)
        print("computing futures")
        try:
            y = client.gather(futures)
            print(y)
            # Sum the tuples in y
            summed_tuples = [sum(t) for t in zip(*y)]

            # Print the summed tuples
            print ("chunk summed tuples")
            print(summed_tuples)
        except:
            print("error")
            pass
    print('unit test' )
    asciigram = myfoo_nodelay(str2)
    print(asciigram)
    print("length of asciigram = " + str(len(asciigram)))
    print(ngrams(word_tokenize("this is a fox"),10))
    unittest = validate_match_nodelay(myfoo_nodelay(str1), str2)
    print(unittest)
    print('Execution cluster close!')
    cluster.close()
