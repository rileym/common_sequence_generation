from itertools import imap
import re

from dto import ShingleRecord

class Shingler(object):

    def __init__(self, shingle_size, normalization_fn, token_ptrn = r"(?u)\b\w\w+\b"):
        '''
        shingle_size:       Number of tokens per shingle (after normalization / filtering)
        normalization_fn:   Function taking a string and returning the normalized version.
                            The normlization function is applied to each token after tokenization. If a raw 
                            token is normalized to the empty string, it is removed from the token sequence.
        token_ptrn:         Regular expression that defines a token. 
        '''
        compiled_token_ptrn = re.compile(token_ptrn)
        self._tokenizer = lambda s: compiled_token_ptrn.findall(s)
        self._shingle_size = shingle_size
        self._normalization_fn = normalization_fn

    def _tokenize(self, doc):
        return self._tokenizer(doc)

    def shingle_doc(self, doc_record):
        '''
        Takes a document (in the form of a DocRecord) and generates the shingles as defined by
        the token_ptrn, shingle_size, and normalization_fn. Shingles are emitted as ShingleRecord
        objects (named tuples: shingle_record.doc_id) 
        doc_record: a DocRecord object (doc_record.doc_id, doc_record.doc) 
        '''
        tokens = self._tokenize(doc_record.doc)
        tokens = imap(self._normalization_fn, tokens)
        tokens = filter(None, tokens)
        n_tokens = len(tokens)
        for i in xrange(n_tokens - self._shingle_size + 1):
            shingle = u' '.join(tokens[i:(i+self._shingle_size)])
            yield ShingleRecord(
                    doc_id = doc_record.doc_id, 
                    i = i,
                    shingle = shingle
                )

    def shingle_docs(self, doc_record_iter):
        for doc_record in doc_record_iter:
            for shingle_record in self.shingle_doc(doc_record):
                yield shingle_record