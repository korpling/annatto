from collections import defaultdict
from graphannis.graph import GraphUpdate
from graphupdate_util import *
import logging
import re


_PROP_TEXT_NAME = 'text_name'
_PROP_ANNO_NS = 'anno_ns'
_PROP_EDGE_NS = 'edge_layer'
_FILE_ENDINGS = ('.ptb',)
_FIXED_SEQUENCES = {
    '-LRB-': '(',
    '-RRB-': ')'
}
_DEFAULT_CAT_NAME = 'cat'


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
_handler = logging.StreamHandler()
_handler.setLevel(logging.DEBUG)
_logger.addHandler(_handler)


def clean_text(text):
    for k, v in _FIXED_SEQUENCES.items():
        text = text.replace(k, v)
    return text


def map_document(u, path, doc_path, cat_name=_DEFAULT_CAT_NAME, text_name='', anno_ns=None, edge_layer=None):
    with open(path) as f:
        data = f.read()
    stack = []
    children = []
    val = ''
    s_count = 0
    tokens = []
    data = re.sub(r'\s+', ' ', data)
    index_stack = []
    covered_tokens = defaultdict(list)
    for c in data:
        if c == '(':
            if (not stack or stack[-1]):                
                stack.append(())  # push
                index_stack.append(len(children))
        elif c == ')':
            # pop
            if val:                
                s_count += 1
                stack[-1] += (val,)
                val = ''
                cat, text = stack.pop()
                index_stack.pop()
                token_id = map_token(u, doc_path, len(tokens) + 1, text_name, clean_text(text))
                tokens.append(token_id)
                struct_id = map_hierarchical_annotation(u, doc_path, s_count, '' if anno_ns is None else anno_ns, cat_name, cat, '' if edge_layer is None else edge_layer, token_id)                
                covered_tokens[struct_id].append(token_id)
                children.append(struct_id)
            elif stack and stack[-1]:
                s_count += 1
                (cat,) = stack.pop()                
                child_index = index_stack.pop()
                struct_id = map_hierarchical_annotation(u, doc_path, s_count, '' if anno_ns is None else anno_ns, cat_name, cat, '' if edge_layer is None else edge_layer, *children[child_index:])
                for child in children[child_index:]:
                    covered_tokens[struct_id].extend(covered_tokens[child])
                children = children[:child_index]
                children.append(struct_id)
        elif c == ' ':
            if val:
                stack[-1] += (val,)
                val = ''
        else:
            val += c
    for dominating_node, dominated_tokens in covered_tokens.items():
        coverage(u, [dominating_node], dominated_tokens)
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
        map_document(u, path, internal_path, text_name=safe_props[_PROP_TEXT_NAME], anno_ns=safe_props[_PROP_ANNO_NS], edge_layer=safe_props[_PROP_EDGE_NS])
    return u
