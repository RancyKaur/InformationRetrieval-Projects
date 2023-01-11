


'''
This script is the main script that would run both the sub-projects of Project3
It has code for both naive indexer and SPIMI-inspired procedure
It starts with main function all that checks if corpus exists in the location, if not, download it
Then move on to create inverted index using SPIMI technique
'''



import subprocess,re,os, time, pickle, importlib
import numpy as np
from nltk import word_tokenize

from importlib import import_module
requiredmodules = ['nltk','requests','tarfile']    
for mods in requiredmodules:
    try:
        import_module(mods)
    except:
        subprocess.check_call(['pip3','install','--user',mods],stdout=subprocess.DEVNULL)
import requests, nltk, tarfile
from collections import Counter

import QueryScript
from QueryScript import executeQueries
importlib.reload(QueryScript)
import OKAPI_Script
importlib.reload(OKAPI_Script)
from OKAPI_Script import *


# This function is to download REUTERS21578 if it does not already exist on the system in the current directory

def downloadReutersColl():
    print("Required modules imported")
    downloadUrl="http://www.daviddlewis.com/resources/testcollections/reuters21578/reuters21578.tar.gz"
    cwd = os.getcwd()
    dirName = "ReuterOriginalCollection"
    absdirPath = os.path.join(cwd,dirName)
    if not os.path.isdir(absdirPath):
        os.mkdir(absdirPath)
    with requests.get(downloadUrl , stream=True) as rx  , tarfile.open(fileobj=rx.raw  , mode="r:gz") as  tarobj  : 
        tarobj.extractall(path=absdirPath) 
    print("The collection is downloaded to: ", absdirPath)
    return absdirPath



# This function works on subproject I of Project 3
# It generates a sub-corpus of docid and rawText until 10K token limit is reached 
# This rawText is then passed to NAIVE and SPIMI Indexer to compare time of exectuion

# Simultaneously it also works on created invertedIndex using SPIMI that would be used for Subproject II of Project 3
# The inverted index basically contains term and a tuple of (docid and tf_d)

def indexTerms(path,rawStuff,invertedIndex,doc_length,totalStats):

        # get all the text from the file at given path
        with open(path, "r") as file: 
            line = file.read()
        file.close()

            # remove the DOCTYPE tag
        i=line.find("<REUT") 
        line = line[i:]

        tok = nltk.regexp_tokenize(line,'(.*?)</REUTERS\>')

        for t in tok:
                #p=nltk.regexp_tokenize(t,'NEWID="(.*?)"\>') # one way to tokenize but then it gives list of size 1
            id1=t.find("NEWID")+7
                #print(type(int(t[p:p+9][-2]))) #other way to use inbuilt string find
            si=t.find("<BODY>")
            ei=t.find("</BODY>")
            id2=t.find("<DATE>")-3

            docid=t[id1:id2] #pulling new docid from text 

            ti = int(si)

                # only get rawtext of those articles that have body tag
            if ti!=-1:
                rawtext=t[si+6:ei] # pull raw text to tokenize

                    # Remove junk characters and punctuations
                rawtext=re.sub('&lt;', '', rawtext)
                rawtext=re.sub('&#[0-9]*;', '', rawtext)
                rawtext=re.sub('[?,;:!"\'>.<*]', '', rawtext)
                rawtext=re.sub('Reuter', '', rawtext)
                
                #tokenize raw text
                tokens=word_tokenize(rawtext)
                r = re.compile("[\/&+$(-)--]") #remove special characters
                rtokens = list(filter(r.match, tokens)) 

                tokens = [item for item in tokens if item not in rtokens]
                rawText2= ""
                
                #Below code is to generate statistics for BM25
                doc_length[int(docid)] = len(tokens) # store length of document in the doc_length dictionary
            
                totalStats[0] += len(tokens)
                totalStats[1] +=1
            
                # generate text for 
                if rawStuff[0] <= 10000:
                    for token in tokens:
                        rawText2 = " ".join(token)
                        if rawStuff[0] < 10000:
                            rawStuff[0]=rawStuff[0]+1
                    rawStuff[1][docid]=rawText2
                    
                    
                # below code simultaneoulsy generates final inverated index using SPIMI
                
                # I create a tuple of (term, count of the term in the document) since the count is needed to calculate BM25 rank
                terms = [(token,tokens.count(token)) for token in set(tokens)]
                docid=int(docid)
                SPIMI_Indexer(terms,docid,invertedIndex)
                
        return rawStuff


# Naive Indexer that takes docid-rawtext pairs and tokenize the raw text and saves in  temporary lsit
# That list is then sorted
# Then it uses this list to created inverted index

def naiveIndexer(rawdict):
    
    starttime = int(round(time.time() * 1000))
    
    tokenDocList=[]
    for k,v in rawdict.items():
        tokens=word_tokenize(v)
        for token in tokens:
            tokenDocList.append(token + " "+ k)
    
    tokenDocList.sort()
    tokendic = {}
    for token in tokenDocList:
        tok = token.split(" ")
        if tok[0] in tokendic:
            post_list=tokendic.get(tok[0])
            post_list.append(tok[1])
        else:
            post_list = []
            post_list.append(tok[1])
            tokendic[tok[0]] = post_list
    endtime = int(round(time.time() * 1000))
    
    return (endtime - starttime)




# This function is to index tokens using SPIMI technique
def spimi10KIndexer(rawdict):
    
    starttime = int(round(time.time() * 1000))
    tokendic={}
    
    for k,v in rawdict.items():
        tokens=word_tokenize(v)
        for token in tokens:
            if token in tokendic:
                post_list=tokendic.get(token)
                post_list.append(k)
            else:
                post_list = []
                post_list.append(k)
                tokendic[token] = post_list
                
    endtime = int(round(time.time() * 1000))
    
    return (endtime - starttime)


# This is the main SPIMI indexer that takes terms and docid as input and 
# outputs tuple of (document id, count of tokens) as postings in the postings list in inverted index
def SPIMI_Indexer(terms,docid,invertedIndex):
    docid = int(docid)
    for term in terms:
        if term[0] in invertedIndex:
            post_list=invertedIndex.get(term[0])
            post_list.append((docid,term[1]))
        else:
            post_list = []
            post_list.append((docid,term[1]))
            invertedIndex[term[0]] = post_list  




# This is the main function that calls function that checks if corpus exists if not download it

def main():

    #The script starts with downloading Reuters corpus and will download to the location only if it does not exist
    CorpusPath = downloadReutersColl()
    
    # get name of all the files with .SGM extension
    SGMfiles = [eachfile for eachfile in os.listdir(CorpusPath) if eachfile.endswith('.sgm')]
    
    #list10K = []    
    rawStuff=[0,{}]
    invertedIndex = {} # Stores entire invereted index
    doc_length = {} # Dictionary to store docid with length of document
    
    # This will finally contain total number of tokens in collection & total number of documents in collection
    totalStats=[0,0,0]
    
    for everyfile in SGMfiles:
        filepath = os.path.join(CorpusPath,everyfile)
        
        rawStuff = indexTerms(filepath,rawStuff, invertedIndex,doc_length,totalStats)
    
    totalStats[2]=round(totalStats[0]/totalStats[1])
    
    print(f'Reuters Collection Stats are:\n\tTotal tokens: {totalStats[0]}\n\tTotal documents: {totalStats[1]}\n\tAverage Document Length: {totalStats[2]}\n')
    pickle.dump(doc_length, open("doc_length.p","wb"))
    pickle.dump(invertedIndex, open("invertedIndex.p","wb"))
    
    # below code is for section 1A of project3 that is to compare timings for 10K terms using Naive & SPIMI
    if rawStuff[0] == 10000:
        naiveTime = naiveIndexer(rawStuff[1])
        spimiTime = spimi10KIndexer(rawStuff[1])
        print('CPU Execution time with NAIVE Indexer:', naiveTime, 'milliseconds')
        print('CPU Execution time with SPIMI Indexer:', spimiTime, 'milliseconds')
        
        if spimiTime < naiveTime:
            print("SPIMI is faster than NAIVE process by: ",naiveTime-spimiTime, " milliseconds for indexing 10K terms\n")
    
        
    # SUBPROJECT II 
    # prompt user to enter search queries and based on their selection do the needful
    # QueryScript and OKAPI_Script has function to process search queries
    
    # Prompt user to do UNRANKED Search
    searchObj = executeQueries(invertedIndex)
    while True:
        user_input = input("Would you like to search (Y/N):")
        if user_input == "Y" or user_input.lower() == "y":
            result = searchObj.promptToSearch() #This module resides in QueryScript
            results = result[0]
            print(f"The search result of {result[2]} query of terms {result[1]} is given below: \n")
            print(f"{len(results)} documents found after {result[2]} operation")
            
            print(results)
        else:
            break
    
    # Ranked Search using OKAPI / BM25 Ranking scheme
    bm25Obj = OKAPI(invertedIndex,totalStats,doc_length,1.2,0.75)
    while True:
        options = {"y": True, "n": False}
        user_input = input("Would you like to use Okapi BM25 ranking? [y/n] ")
        if user_input == "Y" or user_input.lower() == "y":
            queryByUser = searchObj.promptUser()
            bm25Obj.rank_bm25(queryByUser)
            #bm25Obj.calculate_tf_idf(queryByUser)
        else:
            break
    


# Initialize function to call main function
if __name__ == "__main__":
    
    main()





