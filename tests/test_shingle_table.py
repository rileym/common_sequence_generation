import sys
sys.path.append('..')

from shingle_table import ShingleTable
from csg import Sequence, CommonSequenceGenerator, SequenceGroup
from shingler import Shingler
from normalizers import BasicNormalizer
from dto import DocRecord, ShingleRecord

from utils.misc import test_suite_from_test_cases

from abc import ABCMeta, abstractproperty
from itertools import product

import unittest


class SequenceTest(unittest.TestCase):

	_test_sequence_kwargs = dict(
								src_doc_id = u'GIP_ELIQUIS_IS_0148569', 
								src_position = 894, 
								target_doc_id = u'GIP_ELIQUIS_IS_0148569', 
								target_position = 975
							)

	_test_sequence_increment_amount = 5

	_test_sequences_kwargs = map(   \
								  lambda args: dict( zip([u'src_doc_id', u'src_position', u'target_doc_id', u'target_position'], args) ),   \
								  product(u'ABCDEFG', xrange(6), u'HIJKLMN', xrange(6))   \
								)

	def setUp(self):
		self._test_sequence = self._create_test_sequence()
		self._test_sequences = self._create_test_sequences()
		self._increment_map = self._build_increment_map(self._test_sequences)

	def _build_increment_map(self, sequences):
		return dict( zip(sequences, xrange(len(sequences))) )

	@classmethod
	def _create_test_sequence(cls):
		return Sequence(**cls._test_sequence_kwargs)

	@classmethod
	def _create_test_sequences(cls):
		return [Sequence(**kwargs) for kwargs in cls._test_sequences_kwargs]

	@staticmethod
	def increment_sequence(seq, n):
		for _ in xrange(n):
			seq.increment_length()

	@classmethod
	def increment_sequences(cls, sequences, increment_map):
		for seq in sequences:
			cls.increment_sequence(seq, increment_map[seq])

	def test_initial_state(self):

		self.assertEqual(self._test_sequence_kwargs[u'src_doc_id'], self._test_sequence.src_doc_id)
		self.assertEqual(self._test_sequence_kwargs[u'src_position'], self._test_sequence.src_position)
		self.assertEqual(self._test_sequence_kwargs[u'target_doc_id'], self._test_sequence.target_doc_id)
		self.assertEqual(self._test_sequence_kwargs[u'target_position'], self._test_sequence.target_position)
		self.assertEqual(1, self._test_sequence.length)
		self.assertEqual(self._test_sequence_kwargs[u'src_position'] + 1, self._test_sequence.src_end_position)
		self.assertEqual(self._test_sequence_kwargs[u'target_position'] + 1, self._test_sequence.target_end_position)

	def test_state_after_increment(self):
		
		self.increment_sequence(self._test_sequence, self._test_sequence_increment_amount)
		self.assertEqual(self._test_sequence.length, self._test_sequence_increment_amount + 1)

	def test_hashing_basic(self):

		try:
			seq_set = set(self._test_sequences)
		except Exception as e:
			self.fail(u'Set construction failed with message:\n'.format(e))

	def test_hashing_after_increment(self):

		seq_set = set(self._test_sequences)
		self.increment_sequences(self._test_sequences, self._increment_map)
		self.assertTrue( all(seq in seq_set for seq in self._test_sequences) )


	def test_raise_on_attr_set(self):
		seq = self._create_test_sequence()
		non_settable_attr_names = [u'src_doc_id',u'src_position',u'target_doc_id',u'target_position',u'length']
		for attr_name in non_settable_attr_names:
			with self.assertRaises(AttributeError):
				setattr(seq, attr_name, 0)


	# test creation / state
		# create a bunch of sequences, edge cases
		# assert state is as expected
	# test state after increment
		# create a bunch of sequences, edge cases
		# increment various times
		# assert state is as expected
	# test hashing basic
		# create a bunch of sequences, edge cases
		# add to a set asser not error
	# test hashing after increment
		# create a bunch of sequences, edge cases
		# insert into a set
		# increment various amoungs
		# assert those sequences are "in" the set
	# test raise on attr setting
		# create a sequence
		# assert raises Attribute error when trying to set attributes

class AbstractSequenceGroupTest(unittest.TestCase):

	__metaclass__ = ABCMeta

	def setUp(self):
		self.test_sequences = self._build_test_sequences()
		self.groups = self._get_sequence_groups()

	def _build_test_sequences(self):

		dummy_src_doc_id = 0
		dummy_target_doc_id = 1
		dummy_target_pos = 0

		sequences = []
		for src_position, length in self._sequence_infos:
			sequence = Sequence(dummy_src_doc_id, src_position, dummy_target_doc_id, dummy_target_pos)
			SequenceTest.increment_sequence(sequence, length - 1)
			sequences.append(sequence)

		return sequences

	@abstractproperty
	def _sequence_infos(self):
		pass

	@abstractproperty
	def _expected_spans(self):
		pass

	@abstractproperty
	def _expected_lengths(self):
		pass

	def _get_sequence_groups(self):
		return SequenceGroup.group_sequences(self.test_sequences)

	def test_span(self):
		self.assertEqual(len(self.groups), len(self._expected_spans))
		for group, expected_span in zip(self.groups, self._expected_spans):
			self.assertEqual(group.span, expected_span)

	def test_length(self):
		self.assertEqual(len(self.groups), len(self._expected_spans))
		for group, expected_length in zip(self.groups, self._expected_lengths):
			self.assertEqual(group.length, expected_length)

class OriginalPaperGroupTest(AbstractSequenceGroupTest):

	@property
	def _sequence_infos(self):
		return [
				(0,2),
				(0,3),
				(1,1),
				(2,3),
				(5,3),
				(6,3)
			   ]


	@property 
	def _expected_spans(self):
		return [(0,5),(5,9)]

	@property 
	def _expected_lengths(self):
		return [5,4]

class EmptyGroupTest(AbstractSequenceGroupTest):

	@property
	def _sequence_infos(self):
		return []

	@property 
	def _expected_spans(self):
		return []

	@property 
	def _expected_lengths(self):
		return []

class OneBigGroupTest(AbstractSequenceGroupTest):

	@property
	def _sequence_infos(self):
		return [(1,3), (1,6), (1,7), (3,2)]

	@property 
	def _expected_spans(self):
		return [(1,8)]

	@property 
	def _expected_lengths(self):
		return [7]

class SingletonGroupTest(AbstractSequenceGroupTest):

	@property
	def _sequence_infos(self):
		return [(1,3)]

	@property 
	def _expected_spans(self):
		return [(1,4)]

	@property 
	def _expected_lengths(self):
		return [3]

class SeriesOfSingletonsGroupsTest(AbstractSequenceGroupTest):

	@property
	def _sequence_infos(self):
		return [(1,3), (5,1), (7,100)]

	@property 
	def _expected_spans(self):
		return [(1,4), (5,6), (7,107)]

	@property 
	def _expected_lengths(self):
		return [3,1,100]

# class ShinglerTest(unittest.TestCase):
# 	pass 
	
# class ShingleTableTest(unittest.TestCase):
# 	pass 
	
class CommonSequenceGeneratorTest(unittest.TestCase):
	
	shingle_size = 8
	doc_ids = [0,1,2]

	doc_0_content = '''
	My name is test. Blah Blah Blah. I will not match anything.
	'''

	doc_1_content = '''
	This document should nearly match another document that I will write below.
	'''

	doc_2_content = '''
	I would say his doc should nearly match another document that I will write below. How's that?
	'''

	def setUp(self):
		self._build_shingle_table()

	def _build_shingle_table(self):
		norm_fn = BasicNormalizer().normalize
		shingler = Shingler(shingle_size = self.shingle_size, normalization_fn = norm_fn, token_ptrn = r"(?u)\b\w+\b")
		doc_records = self._get_doc_records()
		shingles = shingler.shingle_docs(doc_records)
		self._shingle_table = ShingleTable(shingles)

	def _get_doc_records(self):
		doc_texts = [self.doc_0_content, self.doc_1_content, self.doc_2_content]
		return map(DocRecord, self.doc_ids, doc_texts)

	def test_generate_common_sequences(self):
		
		# doc 1 has a matched sequence with doc 2 from position 2 of length 3
		# doc 2 has a matched sequence with doc 1 from position 5 of length 3

		csg = CommonSequenceGenerator(self._shingle_table)

		with self.assertRaises(KeyError):
			groups_0 = csg.generate_common_sequences(0)

		groups_1 = csg.generate_common_sequences(1)
		self.assertEqual(len(groups_1), 1)
		self._assertExpectedSequenceGroup(sequence_group = groups_1[0], 
										  span = (2,5),
										  length = 3
										  )

		groups_2 = csg.generate_common_sequences(2)

		self.assertEqual(len(groups_2), 1)
		self._assertExpectedSequenceGroup(sequence_group = groups_2[0], 
										  span = (5,8),
										  length = 3
										  )

	def _assertExpectedSequenceGroup(self, sequence_group, span, length):
		self.assertEqual(sequence_group.span, span)
		self.assertEqual(sequence_group.length, length)



if __name__ == '__main__':

	sequence_test_cases = [SequenceTest]
	sequence_group_test_cases = [
					OriginalPaperGroupTest,
					EmptyGroupTest,
					OneBigGroupTest,
					SingletonGroupTest,
					SeriesOfSingletonsGroupsTest,
				 ]
	common_sequence_generator_test_cases = [CommonSequenceGeneratorTest]


	sequence_test_suite = test_suite_from_test_cases(sequence_test_cases)
	sequence_group_test_suite = test_suite_from_test_cases(sequence_group_test_cases)
	common_sequence_generator_suits = test_suite_from_test_cases(common_sequence_generator_test_cases)
	test_suites = [
					sequence_test_suite,
					sequence_group_test_suite,
					common_sequence_generator_suits,
				  ]
	compiled_test_suites = unittest.TestSuite(test_suites)
	
	unittest.TextTestRunner(verbosity=2).run(compiled_test_suites)


#
# (start, length)
# (0,2)
# (0,3)
# (1,1)
# (2,3)
# -----
# (5,3)
# (6,3)
#
# () [Empty Sequence]
# -----
#
# (1,3)
# (1,6)
# (1,7)
# (3,2)
# -----
#
# (1,3) [Singleton]
# -----
#
# (1,3)
# -----
# (5,1)
# -----
# (7,100)
# -----
#