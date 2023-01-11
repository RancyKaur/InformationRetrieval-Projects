
from nltk import word_tokenize
import requests, nltk, tarfile,re,os,pickle

#Define class whose object we would call from the main StartIndexer file.
class JustIndexer:
    # This is the core function that is responsible to extract raw text and do cleanup of punctuations from raw text
    # It store the rawtext of the article as value and newID of the article as key in a dictionary. 
    # This temporary dictionary would be used to create term-docid pair list
    
    def extractRawTokens(self,path):

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

                    # Add to dictionary, docid : rawtext
                termdict[docid] = rawtext
                
                #tokenize raw text
                tokens=word_tokenize(rawtext)
                r = re.compile("[\/&+$(-)--]") #remove special characters
                rtokens = list(filter(r.match, tokens)) 

                tokens = [item for item in tokens if item not in rtokens]
                
                for token in tokens:
                    finaltoken = token + " "+ docid
                    tokenDocList.append(finaltoken)
            

    # This function is called once all the SGM files are processed and all terms-docid pairs are generated
    # It removes duplicate term-docid pairs
    # After that we sort this list as that is key step for creating postings list so that docids are saved in ascending order
    def removeDuplicateTokensAndSort(self,tokenDocList):
        uniqueTokens = list(set(tokenDocList))
        uniqueTokens.sort()
        return uniqueTokens

    # This functions is to creat the actual INVERTED INDEX
    # It checks if the term exists, it not it adds term as key and instantiates a list that would be posting list for that term 
    # If term exists, then it fetches postings list and appends the docid to the postings list
    # In the end it persists dictionary to a pickle file on the disk
    def CreateDocIdPostingList(self, uniqueTokens):
        tokendic = {}
        
        for token in uniqueTokens:
                
            tok = token.split(" ")
            if tok[0] in tokendic:
                post_list=tokendic.get(tok[0])
                post_list.append(tok[1])
            else:
                post_list = []
                post_list.append(tok[1])
                tokendic[tok[0]] = post_list
        
        return endTime

    # This is the primary function that is called by the StartIndexer file
    # Note that it initializes an empty tokenDocIDList that is passed recursively to indexTokens function 
    # The idea is that same list is used to store term-docid pair for entire collection
    # Then it calls other functions to removeDuplicateTokensAndSort and create invertedIndex function
    def StartIndexing(self, dirpath):
        #Initialize a list that would store term-docid at the start of indexing of collection
        fileNames = [eachfile for eachfile in os.listdir(dirpath) if eachfile.endswith('.sgm')]
        tokenDocList = []   
       
        for everyfile in fileNames:
            filepath = os.path.join(dirpath,everyfile)
            self.indexTokens(filepath,tokenDocList)

        uniqueTokens = self.removeDuplicateTokensAndSort(tokenDocList)



