# Copyright (C) 2014-2016 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

"""
Adapted from http://norvig.com/spell-correct.html.
"""

from __future__ import print_function, unicode_literals, division, absolute_import

import re, collections

def _train(features):
    model = collections.defaultdict(lambda: 1)
    for f in features:
        model[f] += 1
    return model

_alphabet = 'abcdefghijklmnopqrstuvwxyz'

def _edits1(word):
   splits     = [(word[:i], word[i:]) for i in range(len(word) + 1)]
   deletes    = [a + b[1:] for a, b in splits if b]
   transposes = [a + b[1] + b[0] + b[2:] for a, b in splits if len(b)>1]
   replaces   = [a + c + b[1:] for a, b in splits for c in _alphabet if b]
   inserts    = [a + c + b     for a, b in splits for c in _alphabet]
   return set(deletes + transposes + replaces + inserts)

def _known_edits2(word, NWORDS):
    return set(e2 for e1 in _edits1(word) for e2 in _edits1(e1) if e2 in NWORDS)

def _known(words, NWORDS):
    return set(w for w in words if w in NWORDS)

def correct(word, known_words):
    """
    :param word: Word to correct
    :type word: string
    :param known_words: List of known words
    :type known_words: iterable of strings

    Given **word**, suggests a correction from **known_words**. If no reasonably close correction is found, returns
    **word**.
    """
    NWORDS = _train(known_words)
    candidates = _known([word], NWORDS) or _known(_edits1(word), NWORDS) or _known_edits2(word, NWORDS) or [word]
    return max(candidates, key=NWORDS.get)
