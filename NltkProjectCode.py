'''
    Project 1 - Goal: text preprocessing with NLTK, proofreading results
    Program will download Reuter's Corpus Reuters-21578 http://www.daviddlewis.com/resources/testcollections/reuters21578/ to your Current Working Directory
    with Name ReuterOriginalCollection
    It will create another folder named 'OutPutFiles' that would contain output files from each step of the pipleine
    
    
    The Pipeline is as follows:
    1. It will read the Reuter's collection(the name of which is given as an input) and extract the raw text of each article from the corpus
    2. It will tokenize the extracted raw text
    3. Following which it will lowercase all the tokens
    4. We will then apply Porter stemmer on these tokens
    5. User can input a list of stop words, that will be used to remove those stop words from text.
'''

import subprocess,re,os
from importlib import import_module
requiredmodules = ['nltk','requests','tarfile']    
for mods in requiredmodules:
    try:
        import_module(mods)
        #print(mods)
    except:
        subprocess.check_call(['pip3','install','--user',mods],stdout=subprocess.DEVNULL)

from nltk import word_tokenize
import requests, nltk, tarfile


# Function to download and extract reuters21578 collection to 'ReuterOriginalCollection' folder in current working directory
def downloadReutersColl():
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
	
# Function to write output to text files and return path
# We will save output to ReutersOutputFiles folder under current working directory
def writeOutputFiles(fileName):
    cwd = os.getcwd()
    dirName = "ReutersOutputFiles"
    outputPath = os.path.join(cwd,dirName)
    if not os.path.isdir(outputPath):
        os.mkdir(outputPath)
    
    fileOutputPath = os.path.join(outputPath,fileName)
    
    return fileOutputPath
	
# extractRawText function is to help extract text from given SGM file
# Input Parameters: ReutersCollection Absolute Path and SGM collection file name
# Output: returns path of the raw text file

# Processing:
# There are some junk characters with prefix '&#digit;' E.g. &#5;, we remove these with blanks
# < symbol is not decoded and appears in its encoded format '&lt;' we decode it to < symbol

# We are also saving the rawText in an output file for later reference/usage

def extractRawText(path,fileName):
    filePath = os.path.join(path,fileName)
    
    # read the given file
    with open(filePath, "r") as file: 
        line = file.read()
    
    #To extract raw text we will just replace all the html tags with blank using regular expressions
    tokenizer = nltk.regexp_tokenize(line,'<[^<]+?>', gaps=True) # we now have rawText with all the raw text that contain junk characters too and encoded character
    
    fileName = re.findall(r'[ \w-]+\.',fileName)[0]
    #Save raw text to a file
    opname = 'extractRawText_output_' + fileName+'txt'
    fileOutputPath = writeOutputFiles(opname)
    
    with open(fileOutputPath, 'w+') as text_file:
        for token in tokenizer:
              text_file.write(token)
    text_file.close()
    
    print("The file containing raw text is saved to: ", fileOutputPath)
    
    return fileOutputPath

# processRawText is called by textTokenizer function to process the raw text prior to generating tokens
# In raw text of the collection there are charater strings like &#digit*; that are not needed in the index
# Addtionally there are certain symbols and numbers that user may not be intereseted in
# I replace all these with blanks and then tokenize
# There are dates in the text which are being kept in case user wants to search for date

def processRawText(textRead):
    tz = nltk.regexp_tokenize(textRead,'&#[0-9]*;', gaps=True)
    textRead=""

    for t in tz:
        textRead+=t

    tz1 = nltk.regexp_tokenize(textRead,'&lt;', gaps=True)
    textRead=""
    for t in tz1:
        textRead+=t

        
    tz1 = nltk.regexp_tokenize(textRead,'''[*|-|>|:|.|,]+''', gaps=True)
    textRead=""
    for t in tz1:
        textRead+=t
    
    return textRead
    
# textTokenizer function will tokenize raw text file that is given at the input Path and write tokens to a file
# Input Parameters: absolute URL of the raw text file that is generated from extractRawText() function
# Output: returns path of the file that has tokens of the raw text file

# textTokenizer function will tokenize raw text file that is given at the input Path and write tokens to a file

# Input Parameters: absolute URL of the raw text file that is generated from extractRawText() function
# Output: returns path of the file that has tokens of the raw text file

def textTokenizer(rawFilePath):
    text_file = open(rawFilePath, 'r', encoding='utf-8')
    textRead = text_file.read()
    text_file.close()
    fileName = re.findall(r'_[ \w-]+\.',rawFilePath)[0] #extracting file name from the URL Path

    stats.append(len(word_tokenize(textRead)))
    #using word_tokenize to generate tokens from the file
    textRead = processRawText(textRead)
    fileTokens = word_tokenize(textRead)
    text_file.close()

    opname = 'tokensFromRaw' + fileName + 'txt'
    fileOutputPath = writeOutputFiles(opname)
    chars="abcdedfjghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    spchar="-'``:;"
    nums ="123456789"
    for tokens in fileTokens:
           if (tokens in spchar) or (tokens in chars) or tokens.isdecimal():
                fileTokens.remove(tokens)
    
    stats.append(len(fileTokens))
    
    fileTokens = set(fileTokens) # To create and output list of terms (unique tokens) to file
    
    with open(fileOutputPath, 'w+') as tokenFile:
        for token in fileTokens:
            tokenFile.write(token+"\n")
        tokenFile.close()
    
    print("The file containing TOKENS is saved to: ", fileOutputPath)
    stats.append(len(fileTokens))
    
    return fileOutputPath
	
# lowerCaseTokens function will Normalizing the tokens by making them lower case

# Input Parameters: absolute URL of the file that has tokens generated by Tokenize step of the pipleine
# Output: returns path of the file that has lower casetokens of the raw text file

def lowerCaseTokens(tokenFilePath):
    text_file = open(tokenFilePath, 'r', encoding='utf-8') # Step3: lower case tokens
    readFile = text_file.read()
    alltokens = [tokens for tokens in readFile.split('\n')]
    text_file.close()

    fileName = re.findall(r'_[\w-]+\.',tokenFilePath)[0]
    opname = 'lowerCaseTokensOutput' + fileName + 'txt'
    fileOutputPath = writeOutputFiles(opname)
    
    with open(fileOutputPath, 'w+') as tokenFile:
        for token in alltokens:
            tokenFile.write(token.lower()+"\n")
    tokenFile.close()
    
    print("The file containing Lower-case TOKENS is saved to: ", fileOutputPath)
    return fileOutputPath
	
# applyPorterStemmer function to apply Porter Stemmer for creating equivalence classes of words

# Input Parameters: absolute URL of the tokens file
# Output: returns path of the file that has applied stemming on the words

def applyPorterStemmer(normalizeLowerPath):
    text_file = open(normalizeLowerPath, 'r', encoding='utf-8') # Step3: lower case tokens
    readFile = text_file.read()
    alltokens = [tokens for tokens in readFile.split('\n')]
    text_file.close()
    
    fileName = re.findall(r'_[ \w-]+\.',normalizeLowerPath)[0]
    
    opname = 'stemmedTokens' + fileName + 'txt'
    fileOutputPath = writeOutputFiles(opname)

    porter = nltk.PorterStemmer() #create porter stemmer object to perform stemming on every term
    stemtoken =[]
    for token in alltokens:
        stemtoken.append(porter.stem(token))
    
    # using counter function to find out difference in terms to know how many terms were stemmed.
    from collections import Counter

    c1 = Counter(alltokens)
    c2 = Counter(stemtoken)

    diff = c1-c2
    stats.append(len(list(diff.elements())))
    
    # write only uniqe terms after stemming to ouput file
    alltokens = set(stemtoken)
            
    with open(fileOutputPath, 'w+') as tokenFile:
        for token in alltokens:
            tokenFile.write(token+"\n")
    tokenFile.close()
    stats.append(len(list(alltokens)))
    print("The file containing STEMMED TOKENS is saved to: ", fileOutputPath)
    return fileOutputPath
	
# excludeStopWords is to remove words from the given raw text

# Input Parameters: absolute URL of the token file and list of stop words
# Output: returns path of the file that has excluded stop words from the list of tokens

def excludeStopWords(porterStemmerPath,stopWordList):
    text_file = open(porterStemmerPath, 'r') # Step3: lower case tokens
    textRead = text_file.read()
    alltokens = [tokens for tokens in textRead.split('\n')]
    text_file.close()
    
    fileName = re.findall(r'_[ \w-]+\.',porterStemmerPath)[0]
    
    opname = 'nonStopWord' + fileName + 'txt'
    fileOutputPath = writeOutputFiles(opname)
    count=0
    tokencount=0
    with open(fileOutputPath, 'w+') as tokenFile:
        for token in alltokens:
            if token not in stopWordList:
                tokenFile.write(token+"\n")
                tokencount+=1
            else:
                count+=1
    tokenFile.close()
    
    stats.append(count)
    stats.append(tokencount)

    print("The file containing TOKENS excluding STOPWORDS is saved to: ", fileOutputPath)
    return fileOutputPath

def main():
    path = downloadReutersColl()
    
    fileName = input("Input name of the collection file WITH extension: ")
    word_string = input("Enter stop words separate them by space: ")

    # Split string into words
    wordList = word_string.split(" ")

    print(f"The stop words entered are: {wordList} \n")

    # Executing Pipeline
    
    # Step 1 - Extract raw text from input file
    rawFilePath = extractRawText(path,fileName)
    
    # Step 2 - Tokenize raw Text
    tokenFilePath = textTokenizer(rawFilePath)
    
    # Step 3 - Normalizing the text by making all the text lower case 
    normalizeLowerPath = lowerCaseTokens(tokenFilePath)
    
    # Step 4 - Potter Stemmer
    porterStemmerPath = applyPorterStemmer(normalizeLowerPath)
    
    # Step 5 - Removal stopwords
    #wordList = ['is','to','a','for','on','an','by','the']
    excludeStopWordPath = excludeStopWords(porterStemmerPath,wordList)

    print(f"\n The statistics for the collection is as follows: ")
    print(f"\n ============================================================")
    print(f"\n Total words in the collection including punctuations: {stats[0]}")
    print(f"\n Total tokens in the collection after excluding punctuations: {stats[1]}")
    print(f"\n Total terms after removing duplicates: {stats[2]}")
    print(f"\n Total number of terms that were STEMMED:  {stats[3]}")
    print(f"\n Total number of unique terms after STEMMING:  {stats[4]}")
    print(f"\n Total stop words that were removed:  {stats[5]}")

    print(f"\n Total number of TERMS in index AFTER stopword removal:  {stats[6]}")

stats=[]
if __name__ == "__main__":
    main()
