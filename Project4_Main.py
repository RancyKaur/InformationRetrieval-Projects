'''
This is the main script to run the program
@author - Rancy Chadha (Student ID: 40221591)
The purpose of this script is to crawl ginacody domain and then do clustering with K-Means K=3 and K=6
Post which it uses AFINN sentiment scores to score the clusters
'''

import crawlerFile, os, nltk,re
from time import time
import pandas as pd
from collections import defaultdict
import importlib
importlib.reload(crawlerFile)
from crawlerFile import CrawlerConcordia
from scrapy.crawler import CrawlerProcess
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from afinn import Afinn

'''
extractTextFromArticle() Definition to help extract Text from the HMTL files
@param fileURL takes fileURL from when text needs to be extracted
BeautifulSoup is used to extract actual URL of the page and raw text from the section "content-main"
Beautiful version 4.9 is used that can be installed from https://www.crummy.com/software/BeautifulSoup/bs4/doc/
'''
def extractTextFromArticle(fileUrl):
    #print(fileUrl)
    with open(fileUrl, "r", encoding="utf-8") as file: 
        rawText = file.read()
    file.close()
    bs = BeautifulSoup(rawText,'lxml')

    for line_break in bs.findAll('br'):       # loop through line break tags
        line_break.replaceWith(" ") 

    content = bs.find("section", {"id": "content-main"})
    url = bs.find('meta', attrs={'property': 'og:url'})
    if content:
        info = [url['content'], content.text]
        return info

'''
myCustomTokenizer() function to generate tokens and ensures phone numbers and email addresses are not tokeninzed
'''    
def myCustomTokenizer(textToTokenize):
        tokens=[]
        for sent in nltk.sent_tokenize(textToTokenize):
            if re.match(r'^\w+$', sent):
                for term in nltk.word_tokenize(sent):
                    tokens.append(term)
            else:
                for term in sent.split():
                    tokens.append(term)
        r = re.compile(r'[!\"#\＄%&\'\(\)\*\+,\./:;<=>\?\[\\\]\^_`{\|}~â€™ •]')
        tokens = [re.sub(r,'',item) for item in tokens]
        r2 = re.compile(r"(\b[-']\b)|[\W_]")
        sptokens = list(filter(r2.match, tokens)) 
        tokens = [item for item in tokens if item not in sptokens]

        #write tokens to a text file, for my reference
        cwd = os.getcwd()
        tpath = cwd + "\\tokens.txt"
        with open(tpath, 'a+',encoding="utf-8") as f:
            for token in tokens:
                f.write(token + " ")
        f.close()
        return tokens

'''
createCluster() function is to peform clustering and save clustering information in a dataframe
@param pageDF, is the dataframe that contains document ids and raw text
noOfClusters takes the K value to perform K-Means

It uses TfidfVectorizer to generate features and uses a custom tokenizer that I have defined above
It creates KMeans model depending on the passed parameters

'''
def createCluster(pageDF,noOfClusters):
    vectorizer = TfidfVectorizer(tokenizer=myCustomTokenizer,stop_words='english',max_df=0.5,min_df=5,)
    X_tfidf = vectorizer.fit_transform(pageDF.rawText.astype(str))

    # increase the number of runs with independent random initiations n_init
    kmodel = KMeans(
    n_clusters=noOfClusters,
    max_iter=100,
    n_init=1,
    #init="random"  #n_init - Number of times the k-means algorithm is run with different centroid seeds
    )
    
    kcluster = kmodel.fit(X_tfidf)
    print(f"\nStatistic of the cluster with k={noOfClusters}")
    print(f"=================================================\n")
    print(f"Number of documents: {X_tfidf.shape[0]}, no of terms: {X_tfidf.shape[1]}")
    print(f"Max iterations: {kmodel.max_iter}")
    print(f"Method for initialization: {kmodel.init}")
    print(f"No of times model ran: {kmodel.n_init}")  
    #Code below is to find out documents in the cluster
    docsInclusters = defaultdict(list)
    k = 0;
    for i in kcluster.labels_:
        docsInclusters[i].append(pageDF.iloc[k].name)
        if noOfClusters==3:
            pageDF.loc[pageDF.iloc[k].name,"cluster3"] = i
        if noOfClusters==6:
            pageDF.loc[pageDF.iloc[k].name,"cluster6"] = i
        k += 1
    
    print(f"\nBreakup of documents in for cluster with k={noOfClusters} is given below\n")
    for clust in docsInclusters :
        print(f"Number of documents in cluster is {len(docsInclusters[clust])}")
    printTopTerms(kmodel,vectorizer,noOfClusters)
    return docsInclusters

def printTopTerms(kmodel,vectorizer,noOfClusters):
    print("Top terms per cluster:")
    order_centroids = kmodel.cluster_centers_.argsort()[:, ::-1]
    terms = vectorizer.get_feature_names_out()
    for i in range(noOfClusters):
        print(f"\nCluster : {i}")
        for j in order_centroids[i, :20]:
            print(f"{terms[j]}, ", end=" ")

'''
calcSentimentScore function is called to calculate sentiment scores
@param: docclsuter take value of K which is either K=3 or K=6
        pageDF is the dataframe that has raw text of the page
It calculates sentiment score of each page and then does average of the sentiment score for the cluster and prints it
'''
def calcSentimentScore(doccluser,pageDF):
    afinn = Afinn()
    for clust in doccluser :
        clustscore=0
        for pageid in doccluser[clust] :
                pagetext = (pageDF[pageDF.index == pageid].rawText).to_string()
                pageDF.loc[pageid,"sentiment"] = afinn.score(pagetext)
                clustscore+=afinn.score(pagetext)
                #print(afinn.score(pagetext), end=" ")
        finscore = clustscore/len(doccluser[clust])
        print(f"Sentiment Score of the cluster is: {finscore}")

'''
The start of the script, that first prompts if crawling needs to be done and then prompts also whether clustering needs to be done
The pages are read from 'HTMLFiles' directory in current directory
A dataframe is create to store, url, rawtext and sentimentscores of each page
In the end the dataframe is store in urlWithScores.csv
'''
if __name__ == "__main__":

    # code below is to crawl ginacody subdomain for a given number of files by the user 
    # It then downloads the repsonse body to HTML files in a folder
    tocrawl = (input("Would you like to crawl: (y) or (n): ")).lower()
    if tocrawl == 'y':
        noOfFiles = int(input("Please enter MAX number of HTML files that you would like to crawl/download: "))
        print("The crawler would crawl with 3 seconds delay within each request")
        process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
        'CLOSESPIDER_PAGECOUNT': noOfFiles,
        'CONCURRENT_REQUESTS' : 1,
        'DOWNLOAD_DELAY': 5
        })
        process.crawl(CrawlerConcordia)
        process.start()
        
    # Extract text from all the downloaded files

    #get folder path of the HTMl files
    cwd = os.getcwd()
    dirName = "HTMLFiles"
    absdirPath = os.path.join(cwd,dirName)    
    FilesToWorkWith = [eachfile for eachfile in os.listdir(absdirPath)]

    pageDF = pd.DataFrame(columns=['url', 'rawText','sentiment','cluster3','cluster6'])
    pd.set_option('display.max_colwidth', None)

    for everyfile in FilesToWorkWith:
        fpath = os.path.join(absdirPath,everyfile)
        words = extractTextFromArticle(fpath)
        if words:
            row = {"url":words[0],"rawText":words[1],"sentiment":0,"cluster3":-1,"cluster6":-1}
            pageDF = pd.concat([pageDF, pd.DataFrame([row])],ignore_index=True)

    pageDF.index += 100

    tocluster = (input("Would you like to cluster: (y) or (n): ")).lower()
    if tocluster == 'y':
        # Create cluster
        doccluster3 = createCluster(pageDF,3) # using k=3
        doccluster6 = createCluster(pageDF,6) # using k=6

        print("\n\n************************\n")
        print(f"The sentiment score for cluster with k=3 is given below:\n")
        calcSentimentScore(doccluster3,pageDF)
        print("\n************************\n")
        print(f"The sentiment score for cluster with k=6 is given below:\n")
        calcSentimentScore(doccluster6,pageDF)

        dataframefpath = cwd + "\\urlWithScores.csv"
        pageDF.to_csv(dataframefpath)