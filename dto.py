from collections import namedtuple

# Data Transfer Objects
DocRecord = namedtuple('DocRecord', 'doc_id doc')
ShingleRecord = namedtuple('ShingleRecord', 'doc_id i shingle')