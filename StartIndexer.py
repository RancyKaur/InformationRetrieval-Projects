# StartIndexer that is the primary PY file for execution of all the three subprojects

# Importing libraries to be used for the project
import requests, nltk, tarfile
from nltk import word_tokenize
import subprocess,re,os, time, pickle
from nltk.stem import PorterStemmer
import importlib 

from importlib import import_module
requiredmodules = ['nltk','requests','tarfile']    
for mods in requiredmodules:
    try:
        import_module(mods)
    except:
        subprocess.check_call(['pip3','install','--user',mods],stdout=subprocess.DEVNULL)

import NaiveIndexer

importlib.reload(NaiveIndexer)
from NaiveIndexer import JustIndexer


# Function to download and extract reuters21578 collection to 'ReuterOriginalCollection' folder in current working directory
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

# Function to create dictionary while performing various forms of lossy compression
def createTermDict(termList):
    cdict={}
    for token in termList:
        tok = token.split(" ")
        if tok[0] in cdict:
            post_list=cdict.get(tok[0])
            post_list.append(tok[1])
        else:
            post_list = []
            post_list.append(tok[1])
            cdict[tok[0]] = post_list
    return cdict


# Function that does compression in form of number removal, case folding, removal of stop words and stemming of words
# It takes inverted index from the disk and then after a particular compression is performed it calculates and stores statistics
# Statistics such as number of terms after doing compression, then % of change from overall index and % change from previous compression
# Statistics is calculated for both terms dictionary and postings list and stored in two lists that is used for displaying the result

def implementLossyCompression(termsStats,postingsStats):
    
    #lists that will store statistics for terms and Postings List
    termStatsList=[]
    postingsStatsList=[]
    
    # get the terms - postings list dictionary
    invertedIndex = pickle.load(open("invertedIndex.p","rb"))

    #get UNFILTERED terms and postings list statistics
    unfilteredTerms = len(invertedIndex)
    
    termStatsList.append(unfilteredTerms)
    termStatsList.append("")
    termStatsList.append("")
    termsStats["unfilteredTerms"] = termStatsList
    
    UnfilteredPostingLen = sum(len(v) for i,v in invertedIndex.items())
    postingsStatsList.append(UnfilteredPostingLen)
    postingsStatsList.append("")
    postingsStatsList.append("")
    postingsStats["unfilteredTerms"] = postingsStatsList    
    #===============================================================================================
    
    # REMOVE NUMBERS and get terms and postings list statistics
    noNumList=[]
    tokenDocIDList = pickle.load(open("tokenDocIDList.p","rb"))
    for token in tokenDocIDList:
        tok = token.split(" ")
        if tok[0].isalpha() and not tok[0].isdigit():
            noNumList.append(token)
    #create dictionary with filtered terms and its corresponding postings list
    noNumDict =  createTermDict(noNumList) 
    
    #Generate Statistics
    termStatsList=[]
    termStatsList.append(len(noNumDict))
    termStatsList.append(round(((len(invertedIndex) - len(noNumDict))*100)/ len(invertedIndex)))
    termStatsList.append(round(((len(invertedIndex) - len(noNumDict))*100)/ len(invertedIndex)))
    termsStats["numberRemoved"] = termStatsList
    
    postingsStatsList=[]
    numPostlen= sum(len(v) for i,v in noNumDict.items())
    postingsStatsList.append(numPostlen)
    postingsStatsList.append(round(((UnfilteredPostingLen - numPostlen)*100)/ UnfilteredPostingLen))
    postingsStatsList.append(round(((UnfilteredPostingLen - numPostlen)*100)/ UnfilteredPostingLen))
    postingsStats["numberRemoved"] = postingsStatsList
    #===============================================================================================
    
    # Case Folding and get terms and postings list statistics
    caseFold = [t.lower() for t in noNumList]
    caseFoldDict =  createTermDict(caseFold)
    
    #Generate Statistics
    termStatsList=[]
    termStatsList.append(len(caseFoldDict))
    termStatsList.append(round(((len(noNumDict) - len(caseFoldDict))*100)/ len(noNumDict)))
    termStatsList.append(round(((len(invertedIndex) - len(caseFoldDict))*100)/ len(invertedIndex)))
    
    termsStats["caseFolded"] = termStatsList
    
    postingsStatsList=[]
    casePostlen= sum(len(v) for i,v in caseFoldDict.items())
    postingsStatsList.append(casePostlen)
    postingsStatsList.append(round(((numPostlen-casePostlen)*100)/ numPostlen))
    postingsStatsList.append(round(((UnfilteredPostingLen - casePostlen)*100)/ UnfilteredPostingLen))
    postingsStats["caseFolded"] = postingsStatsList
    
    
    #===============================================================================================
    
    # Remove 30 stop words and get terms and postings list statistics
    stopword30 =['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself']
    
    remove30 = [w for w in caseFold if w.split(" ")[0] not in stopword30]
    remove30Dict =  createTermDict(remove30)
    
    #Generate Statistics
    termStatsList=[]
    termStatsList.append(len(remove30Dict))
    termStatsList.append(round(((len(caseFoldDict) - len(remove30Dict))*100)/ len(caseFoldDict)))
    termStatsList.append(round(((len(invertedIndex) - len(remove30Dict))*100)/ len(invertedIndex)))
    termsStats["remove30"] = termStatsList
    
    postingsStatsList=[]
    r30Postlen= sum(len(v) for i,v in remove30Dict.items())
    postingsStatsList.append(r30Postlen)
    postingsStatsList.append(round(((casePostlen-r30Postlen)*100)/ casePostlen))
    postingsStatsList.append(round(((UnfilteredPostingLen - r30Postlen)*100)/ UnfilteredPostingLen))
    postingsStats["remove30"] = postingsStatsList
    
    
    #===============================================================================================
    
    # Remove 150 stop words and get terms and postings list statistics
    
    stopword150 = ['they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't","example"]
    
    remove150 = [w for w in remove30 if w.split(" ")[0] not in stopword150]
    remove150Dict =  createTermDict(remove150)
    
    #Generate Statistics
    termStatsList=[]
    termStatsList.append(len(remove150Dict))
    termStatsList.append(round(((len(remove30Dict) - len(remove150Dict))*100)/ len(remove30Dict)))
    termStatsList.append(round(((len(invertedIndex) - len(remove150Dict))*100)/ len(invertedIndex)))
    
    termsStats["remove150"] = termStatsList

    postingsStatsList=[]
    r150Postlen= sum(len(v) for i,v in remove150Dict.items())
    postingsStatsList.append(r150Postlen)
    postingsStatsList.append(round(((r30Postlen-r150Postlen)*100)/ r30Postlen))
    postingsStatsList.append(round(((UnfilteredPostingLen - r150Postlen)*100)/ UnfilteredPostingLen))
    postingsStats["remove150Dict"] = postingsStatsList
    
    #===============================================================================================
    
    # Stemming and get terms and postings list statistics
    porter = PorterStemmer()
    stemList = [porter.stem(t.split(" ")[0]) + " " + t.split(" ")[1] for t in remove150]
    stemDict =  createTermDict(stemList)
    
    pickle.dump(stemDict, open("compressedIndex.p","wb"))
    
    with open("stemmedDict.txt", 'a+') as tokenFile:
        for k,v in stemDict.items():
            tokenFile.write(k +"\n")
    tokenFile.close()
    
        
    #Generate Statistics
    termStatsList=[]
    termStatsList.append(len(stemDict))
    termStatsList.append(round(((len(remove150Dict) - len(stemDict))*100)/ len(remove150Dict)))
    termStatsList.append(round(((len(invertedIndex) - len(stemDict))*100)/ len(invertedIndex)))
    
    termsStats["stemList"] = termStatsList
    
    postingsStatsList=[]
    stemPostlen= sum(len(v) for i,v in stemDict.items())
    postingsStatsList.append(stemPostlen)
    postingsStatsList.append(round(((r150Postlen-stemPostlen)*100)/ r150Postlen))
    postingsStatsList.append(round(((UnfilteredPostingLen - stemPostlen)*100)/ UnfilteredPostingLen))
    postingsStats["stemList"] = postingsStatsList
    


# This function is to display the outcome of lossy compression

def printStatistics(termsStats,postingsStats):

    print("Terms Statistics: ")
    print("===============================")
    for k,v in termsStats.items():
        print(f'{k}\t"---"\t{v[0]}\t-{v[1]}\t-{v[2]}')
    print("\nPostings List Statistics: ")
    print("====================================")
    for k,v in postingsStats.items():
        print(f'{k}\t"---"\t{v[0]}\t-{v[1]}\t-{v[2]}')


#This function is to query the inverted index for single term search and it returns corresponding postings list
def processQueryfromIndex(queryTerm):
    print(queryTerm)
    invertedIndex = pickle.load(open("invertedIndex.p","rb"))
    
    if queryTerm in invertedIndex:
        print(invertedIndex.get(queryTerm))
        print("The length of postings list from INVERTED index: ", len(invertedIndex.get(queryTerm)))


#This function is to query the COMPRESSED index for single term search and it returns corresponding postings list
def processQueryfromCompIndex(queryTerm):
    print(queryTerm)
    compressedIndex = pickle.load(open("compressedIndex.p","rb"))
   
    if queryTerm in compressedIndex:
        pl=compressedIndex.get(queryTerm)
        print(pl)
        print("The length of postings list from COMPRESSED index: ", len(pl))
    else:
            print("The term not found in COMPRESSED index")


# This is the main function that calls function StartIndexing from class JustIndexer
# It then calls function to do lossy compression
# It also prompts user if they want to search
# However processquery functions can be called standalone as well

def main():
    CorpusPath = downloadReutersColl()
    
    # get name of SGM files
    SGMfiles = [eachfile for eachfile in os.listdir(CorpusPath) if eachfile.endswith('.sgm')]

    indexObj = JustIndexer()
    startTime = time.process_time()
    indexObj.StartIndexing(SGMfiles,CorpusPath)
    
    endTime = time.process_time()
    
    print('CPU Execution time for indexing entire collection and creating inverted index:', (endTime - startTime), 'seconds')
    
    # Implement lossy compression
    termsStats = {}
    postingsStats ={}

    startTime = time.process_time()
    implementLossyCompression(termsStats,postingsStats)
    endTime = time.process_time()
    
    print('CPU Execution time for implementing Lossy compression:', (endTime - startTime), 'seconds')
    
    # Query Processing prompts to the user
    
    toQuery = input("Would you like to query for any term (Y or N): ")
    while toQuery=='Y':
        queryTerm = input("Enter query term: ")
        processQueryfromIndex(queryTerm)
        compQuery = input("Would you like this term in COMPRESSED index (Y or N): ")
        
        if compQuery=='Y':
            processQueryfromCompIndex(queryTerm) 
        
        toQuery = input("Would you like to query for more terms (Y or N): ")
        if toQuery=='N':
            break
    
    printStatistics(termsStats,postingsStats)
    

# Initialize function to call main function
if __name__ == "__main__":
    
    main()


# queries = ['copper','Samjens','Carmark','Bundesbank']

# for query in queries:
#     processQueryfromIndex(query)
#     processQueryfromCompIndex(query)


# In[ ]:




