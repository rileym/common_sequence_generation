

class Sequence(object):
    
    def __init__(self, src_doc_id, src_position, target_doc_id, target_position):
        self._src_doc_id = src_doc_id
        self._src_position = src_position 
        self._target_doc_id = target_doc_id
        self._target_position = target_position
        self._length = 1

    @property
    def src_doc_id(self):
        return self._src_doc_id

    @property
    def src_position(self):
        return self._src_position

    @property
    def target_doc_id(self):
        return self._target_doc_id

    @property
    def target_position(self):
        return self._target_position

    @property
    def length(self):
        return self._length

    @property 
    def src_end_position(self):
        return self._src_position + self.length

    @property 
    def target_end_position(self):
        return self._target_position + self.length

    @src_doc_id.setter
    def src_doc_id(self, value):
        raise AttributeError('\'src_doc_id\' is read only.')        

    @src_position.setter
    def src_position(self, value):
        raise AttributeError('\'src_position\' is read only.')

    @target_doc_id.setter
    def target_doc_id(self, value):
        raise AttributeError('\'target_doc_id\' is read only.')        

    @target_position.setter
    def target_position(self, value):
        raise AttributeError('\'target_position\' is read only.')

    @length.setter 
    def length(self, value):
        raise AttributeError('\'length\' is read only. Use \'increment\' to increment.')

    def increment_length(self):
        self._length = self._length + 1

    def __hash__(self):
        return hash((self.src_position, self.target_doc_id, self.target_position))

    def __eq__(self, other):
        return (
                self._src_doc_id == other._src_doc_id and 
                self._src_position == other._src_position and
                self._target_doc_id == other._target_doc_id and 
                self._target_position == other._target_position and
                self._length == self._length
                )

    def __repr__(self):
        fmt_str = u'''{class_}(length={length}, source_key=({src_doc_id}, {src_position}), target_key=({target_doc_id}, {target_position}))'''

        return fmt_str.format(
                                class_ = self.__class__.__name__,
                                src_doc_id = self._src_doc_id,
                                src_position = self._src_position,
                                target_doc_id = self._target_doc_id,
                                target_position = self._target_position,
                                length = self._length
                             )


# TODO: functionality to recover source text from Sequence object
# TODO: make adjustments for implementation on distributed system / spark
# TODO: uniform object for shingle -- either tuple or named tuple (ShingleRecord vs whats in inverted_shingle_table)
class CommonSequenceGenerator(object):


    def __init__(self, shingle_table):
        '''
        shingle_table: shingle:     shingle -> set( [(doc_id_1, position_1), ..., (doc_id_n, position_n)] )
        ( shingle_table.invert():   doc_id -> [(shingle_1, position_1), ..., (shingle_m, position_m)], where i > j => position_i > position_j )
        '''
        self._shingle_table = shingle_table
        self._inverted_shingle_table = shingle_table.invert()


    def generate_common_sequences(self, doc_id):
        '''
        Given a doc_id, produce a list sequences shared between the 
        specified document and other documents in the corpus implicit 
        in this instance's shingle table.
        '''

        common_seqs = []  
        active_seqs = set()

        src_shingles = self._inverted_shingle_table[doc_id]
        prev_src_shingle = None
        for src_shingle in src_shingles: # src_shingle = (text, i)
            src_shingle_text, src_position = src_shingle
            if not self._consecutive(prev_src_shingle, src_shingle):
                self._close_active_seqs(active_seqs, common_seqs)

            target_shingle_locs = self._shingle_table[src_shingle_text].copy() # .copy() if generate_common_sequences used multiple times
            self._process_active_seqs(active_seqs, common_seqs, target_shingle_locs)

            self._process_target_shingles(active_seqs, target_shingle_locs, doc_id, src_position, src_shingles)

            prev_src_shingle = src_shingle

        self._close_active_seqs(active_seqs, common_seqs)

        return SequenceGroup.group_sequences(common_seqs)

    def _consecutive(self, prev_src_shingle, cur_src_shingle):

        if prev_src_shingle is not None:
            return prev_src_shingle[1] == (cur_src_shingle[1] - 1)

        return True


    def _process_active_seqs(self, active_seqs, common_seqs, target_shingle_locs):
        
        seqs_to_discontinue = set()
        for seq in active_seqs:
            self._process_active_seq(seq, active_seqs, common_seqs, target_shingle_locs, seqs_to_discontinue)

        active_seqs.difference_update(seqs_to_discontinue)

    def _process_active_seq(self, seq, active_seqs, common_seqs, target_shingle_locs, seqs_to_discontinue):

        next_target_shingle_loc = self._get_next_target_shingle_loc(seq)

        # Common sequence between src and trg continues.
        if next_target_shingle_loc in target_shingle_locs:
            seq.increment_length()
            target_shingle_locs.remove(next_target_shingle_loc) # So that you don't start a new sequence 
                                                             # for this shingle in _process_target_shingles.

        # Common sequence between src and trg ends.
        else:
            common_seqs.append(seq)
            seqs_to_discontinue.add(seq)

    def _get_next_target_shingle_loc(self, sequence):

        next_target_shingle_pos = sequence.target_position + sequence.length
        return (sequence.target_doc_id, next_target_shingle_pos)

    def _process_target_shingles(self, active_seqs, target_shingle_locs, doc_id, src_position, src_shingles):
        # All target shingles remaining in target_shingle_locs represent 
        # the first shingle in the start of a new common sequence.
        
        for (target_doc_id, target_position) in target_shingle_locs:
            if (target_doc_id, target_position) not in src_shingles:
                new_seq = Sequence( src_doc_id = doc_id, 
                                    src_position = src_position, 
                                    target_doc_id = target_doc_id, 
                                    target_position = target_position)
                active_seqs.add(new_seq)

    def _close_active_seqs(self, active_seqs, common_seqs):
        common_seqs.extend(active_seqs)
        active_seqs.clear()

class SequenceGroup(object):

    def __init__(self, seed_sequence):

        self._start_position = seed_sequence.src_position
        self._end_position = seed_sequence.src_end_position
        self._active_sequence = seed_sequence
        self._sequences = set([seed_sequence])

    @property 
    def active_sequence(self):
        return self._active_sequence

    @property
    def span(self):
        return (self._start_position, self._end_position)

    @property
    def length(self):
        return self._end_position - self._start_position

    def add_sequence(self, sequence):

        if not self.includes(sequence):
            raise ValueError('Attempting to add sequences that does not overlap with the active sequence.')

        self._update_active_sequence(sequence)
        self._update_start_position(sequence)
        self._update_end_position(sequence)
        self._add_sequence(sequence) 

    def _update_active_sequence(self, new_sequence):
        if self._subsumes(new_sequence, self._active_sequence):
            self._active_sequence = new_sequence

    def _update_start_position(self, new_sequence):
        self._start_position = min(self._start_position, new_sequence.src_position)

    def _update_end_position(self, new_sequence):
        self._end_position = max(self._end_position, new_sequence.src_end_position)

    def _add_sequence(self, new_sequence):
        self._sequences.add(new_sequence)

    def includes(self, candidate_sequence):
        return self._overlaps(self.active_sequence, candidate_sequence)

    def _overlaps(self, left_seq, right_seq):
        return left_seq.src_end_position > right_seq.src_position

    def _subsumes(self, left_seq, right_seq):
        return ( (left_seq.src_position == right_seq.src_position) and
                (left_seq.src_end_position > right_seq.src_end_position) )

    def __repr__(self):
        return self._sequences.__repr__()


    @staticmethod
    def group_sequences(ordered_sequences):
        
        if not ordered_sequences:
            return []

        active_group = SequenceGroup(ordered_sequences.pop(0))
        groups = [active_group]
        for next_sequence in ordered_sequences:
            if active_group.includes(next_sequence):
                active_group.add_sequence(next_sequence)

            else:
                active_group = SequenceGroup(next_sequence)
                groups.append(active_group)

        return groups
