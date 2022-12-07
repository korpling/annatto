from functools import partial
from graphannis.graph import GraphUpdate
from graphupdate_util import *
import os


_FILE_ENDINGS = ('.ptb')
_FIXED_SEQUENCES = {
    '-LRB-': '(',
    '-RRB-': ')'
}
_DEFAULT_CAT_NAME = 'cat'


def map_document(u, path, doc_path, cat_name=_DEFAULT_CAT_NAME):
    with open(path) as f:
        data = f.read()
    stack = []
    children = []
    val = ''
    t_count = 0
    s_count = 0
    for c in data:
        if c == '(':
            stack.push(())  # push
        elif c == ')':
            # pop
            if val:
                t_count += 1
                s_count += 1
                stack[-1] += (val,)
                val = ''
                cat, text = stack.pop()
                token_id = map_token(u, doc_path, t_count, None, text)
                struct_id = map_hierarchical_annotation(u, doc_path, s_count, None, cat_name, cat, [token_id])
                children.append(struct_id)
            else:
                s_count += 1
                (cat,) = stack.pop()
                struct_id = map_hierarchical_annotation(u, doc_path, s_count, None, cat_name, cat, children)
                children = [struct_id]
        elif c == ' ':
            if val:
                stack[-1] += (val,)
                val = ''
        else:            
            val += c


def start_import(path, **properties):
    """
    Import all conll documents in the given directory.
    >>> type(start_import('test/ptb/importer')).__name__
    'GraphUpdate'
    """
    u = GraphUpdate()
    for path, internal_path in path_structure(u, path, _FILE_ENDINGS):
        pass
    return u
