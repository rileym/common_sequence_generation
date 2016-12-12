from abc import ABCMeta, abstractproperty

# Abstract Normalizer
class AbstractNormalizer(object):

    ___metaclass__ = ABCMeta

    @abstractproperty 
    def _normalizer_fns(self):
        pass

    def normalize(self, s):
        return reduce(lambda s, f: f(s), self._normalizer_fns, s)


# Implementations
ONE_OR_MORE_DIGITS_RE = '\d+'
NON_ALPHANUMERIC = '[^a-zA-Z]+'

def regexep_replace_closure(re_ptrn, repl):

    compiled_regexp = re.compile(re_ptrn)
    def regexep_replace(s):
        return compiled_regexp.sub(repl, s)

    return regexep_replace

class BasicNormalizer(AbstractNormalizer):

    @property
    def _normalizer_fns(self):
        return [ 
                    methodcaller('lower'),
                    space_normalizer,
                    regexep_replace_closure(ONE_OR_MORE_DIGITS_RE, u''),
                    regexep_replace_closure(NON_ALPHANUMERIC, u''),
               ]
