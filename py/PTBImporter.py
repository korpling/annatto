from collections import defaultdict
from graphannis.graph import GraphUpdate
from graphupdate_util import *
import re


_PROP_TEXT_NAME = 'text_name'
_PROP_ANNO_NS = 'anno_ns'
_FILE_ENDINGS = ('.ptb',)
_FIXED_SEQUENCES = {
    '-LRB-': '(',
    '-RRB-': ')'
}
_DEFAULT_CAT_NAME = 'cat'


def map_document(u, path, doc_path, cat_name=_DEFAULT_CAT_NAME, text_name='', anno_ns=None):
    with open(path) as f:
        data = f.read()
    stack = []
    children = []
    val = ''
    s_count = 0
    tokens = []
    data = re.sub(r'\s+', ' ', data)
    for c in data:
        if c == '(':
            stack.append(())  # push
        elif c == ')':
            # pop
            if val:
                s_count += 1
                stack[-1] += (val,)
                val = ''
                cat, text = stack.pop()
                token_id = map_token(u, doc_path, len(tokens) + 1, text_name, text)
                tokens.append(token_id)
                struct_id = map_hierarchical_annotation(u, doc_path, s_count, '' if anno_ns is None else anno_ns, cat_name, cat, '' if anno_ns is None else anno_ns, token_id)
                children.append(struct_id)
            elif stack and stack[-1]:
                s_count += 1
                (cat,) = stack.pop()
                struct_id = map_hierarchical_annotation(u, doc_path, s_count, '' if anno_ns is None else anno_ns, cat_name, cat, '' if anno_ns is None else anno_ns, *children)
                children = [struct_id]
        elif c == ' ':
            if val:
                stack[-1] += (val,)
                val = ''
        else:            
            val += c
    add_order_relations(u, tokens)
    if text_name:
        add_order_relations(u, tokens, text_name)


def start_import(path, **properties):
    """
    Import all ptb documents in the given directory.
    >>> type(start_import('test/import/ptb')).__name__
    'GraphUpdate'
    """
    u = GraphUpdate()
    safe_props = defaultdict(type(None), properties)
    for path, internal_path in path_structure(u, path, _FILE_ENDINGS):
        map_document(u, path, internal_path, text_name=safe_props[_PROP_TEXT_NAME], anno_ns=safe_props[_PROP_ANNO_NS])
    return u
