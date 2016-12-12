from operator import itemgetter
from itertools import imap
from normalizers import BasicNormalizer
from dto import DocRecord, ShingleRecord

# parse
# normalize
# shingle

# TODO: decide where to enforce unicode / request encoding
# TODO: normalization should be the same for reference set matching and normalization here 
# (i.e. use the same normalization code -- at least with respect to non-whitespace chars)
# TODO: normalization should be idempotent


# common_seqs must support:
# - iteration
# - add element
# active_seqs must support:
# - iteration
# - add element
# - pop / remove element 
# sequence must support:
# - read access to target position, length, target doc id
# - write access (increment)
# - hashable

class ShingleTable(dict):

    def __init__(self, shingle_record_iter):

        self._build_table(shingle_record_iter)

    def _build_table(self, shingle_record_iter):

        self._add_shingles(shingle_records = shingle_record_iter)
        self._purge_uniques()
        
    def _add_shingles(self, shingle_records):

        for shingle_record in shingle_records:
            self._add_shingle(
                                 doc_id = shingle_record.doc_id,
                                 i = shingle_record.i,
                                 shingle = shingle_record.shingle
                             )

    def invert(self):

        inverted = {}
        for shingle, bucket in self.iteritems():
            for (doc_id, i) in bucket:
                inverted.setdefault(doc_id, []).append( (shingle, i) )

        return self._sort_inverted(inverted)

    def _add_shingle(self, doc_id, i, shingle):

        self.setdefault(shingle, set()).add( (doc_id, i) )

    def _purge_uniques(self):

        purge_keys = [shingle for shingle, bucket in self.iteritems() if len(bucket) < 2]
        for shingle in purge_keys:
            self.pop(shingle)

    def _sort_inverted(self, inverted):

        inverted_sorted = {}
        for doc_id, inv_buckets in inverted.iteritems():
            inverted_sorted[doc_id] = sorted( inv_buckets, key = itemgetter(1) )

        return inverted_sorted

