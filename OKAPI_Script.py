'''
The equation we will model is the following:
        ∑ (for term in query) [ log10(N / df_t) * ( (k1 + 1)f_td / (k1((1 - b) + b * (L_d / L_ave)) + f_td ) ]
        
        Below are the components of the equation:
        
        N : total number of documents in the collection
        df_t : number of documents the term appears in
        TF_td : number of times the term appears in a document
        
        k1 : Positive parameter used to scale the document frequency scaling.
             k1 = 0 corresponds to a binary model (no term frequency).
             I will try using 0.5

        b : 0 ≤ b ≤ 1. Used to determine the scaling by document length.
                  b = 1 corresponds to fully scaling the term weight by the document length.
                  b = 0 corresponds to no length normalization.
            I will use 0.5
            
        L_d : Length of the document
        L_ave : Average document length for whole collection 
        
        I have defined functions to calculate BM25 rank for each team and then SUM the ranks
        The calculation is as follows:
        
        calculate_idfWeight(queryterm) * calculate_num(queryterm, doc_id) / calculate_denom(queryterm, doc_id)
'''

import QueryScript,importlib
from QueryScript import executeQueries
importlib.reload(QueryScript)
from math import log10
from operator import itemgetter

class OKAPI:

    # Initates variables with object creation that would be used to calculate BM25 Rank
    def __init__(self,invertedIndex, stats,document_lengths,k1,b):
        self.index = invertedIndex;
        self.queryObj = executeQueries(invertedIndex)
        self.document_lengths = document_lengths # This dictionary would help us get L_d
        self.total_documents = stats[1]  # here we  are storing N
        self.avg_docLength = stats[2]   # this represents L_ave
        self.k1 = k1                    
        self.b = b                    
    
    '''
    rank_bm25 is the main function that would calculate BM25 rank for the queries
    
    First we will do OR query and get union of all the postings for each query term
    '''
    def rank_bm25(self, queryByUser):
        
        queryTerms = queryByUser.split(" ")
        docIds = self.queryObj.ORQueryExecution(queryTerms)[0]
        term_rank = {}

        for perDocID in docIds:
            score = 0
            for queryterm in queryTerms:
                score += self.calculate_idfWeight(queryterm) * self.calculate_num(queryterm, perDocID) / self.calculate_denom(queryterm, perDocID)
            term_rank[perDocID] = score

        sorted_rank = sorted(term_rank.items(), key=itemgetter(1), reverse=True)
        
        sorted_rank_top10 = sorted_rank[0: 10]
        print("{} documents found.".format(len(docIds)))
        
        print("Displaying BM-25 Ranking for result documents: ")
        for k, v in sorted_rank:
            print("Document {} score: {}".format(k, v))
        print()
        
    # Get the inverse document frequency weight of a term. 
    def calculate_idfWeight(self, queryterm):
        dft = self.get_document_frequency(queryterm)
        try:
            return log10(self.total_documents / dft)
        except ZeroDivisionError:
            return 0

    def calculate_num(self, queryterm, perDocID):
        return (self.k1 + 1) * self.get_frequency_of_term_in_doc(queryterm, perDocID)

    def calculate_denom(self, queryterm, perDocID):
        return self.k1 * ((1 - self.b) + self.b * (self.document_lengths[perDocID] / self.avg_docLength)) + self.get_frequency_of_term_in_doc(queryterm, perDocID)
    
    
    def get_frequency_of_term_in_doc(self, queryterm, perDocID):
       postings_list = self.index.get(queryterm)
       count = [count for (docid,count) in postings_list if docid == perDocID]
       if len(count)>0:
           count = count[0]
       else:
            count=0
       return count

    def get_document_frequency(self, queryterm):
        postings_list = self.index.get(queryterm)
        return len(postings_list)
        
    
    def calculate_tf_idf(self, queryByUser):
        queryTerms = queryByUser.split(" ")
        docIds = self.queryObj.ORQueryExecution(queryTerms)[0]
        term_rank = {}

        for perDocID in docIds:
            score = 0
            for queryterm in queryTerms:
                score += self.get_frequency_of_term_in_doc(queryterm, perDocID) * self.calculate_idfWeight(queryterm)
            term_rank[perDocID] = score

        sorted_rank = sorted(term_rank.items(), key=itemgetter(1), reverse=True)
        
        sorted_rank_top10 = sorted_rank[0: 10]
        print("{} documents found.".format(len(docIds)))
        
        print("Displaying TF-IDF of  TOP 10 documents: ")
        for k, v in sorted_rank_top10:
            print("Document {} score: {}".format(k, v))
        print()