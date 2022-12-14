from collections import defaultdict, namedtuple
from glob import iglob
from graphannis.graph import GraphUpdate
import logging
import sys

from graphupdate_util import *

# logger
_logger = logging.getLogger(__name__)
_handler = logging.FileHandler('conll-importer.log')
_handler.setLevel(logging.INFO)
_stream = logging.StreamHandler(stream=sys.stdout)
_stream.setLevel(logging.INFO)
_logger.setLevel(logging.INFO)
_logger.addHandler(_handler)
_logger.addHandler(_stream)

# properties
PROPERTY_TEXT_NAME = 'text_name'
PROPERTY_SKIP_NAMED_ORDERING = 'skip_named_ordering'
PROPERTY_ANNO_NS = 'anno_ns'

_FIELD_NAMES = [
    'id',
    'form',
    'lemma',
    'upos',
    'xpos',
    'feats',
    'head',
    'deprel'
]
_NONE = '_'
_FEAT_SEP = '|'
_FUNC = 'func'
_TYPE_DEP = 'dep'
_ANNO_NAME_DEPREL = 'deprel'
_FILE_ENDINGS = ('.conll', '.conllu', '.txt')
_META_MARKER = '#'
_ANNO_NAME_S = 'sentence'

_Token = namedtuple('Token', _FIELD_NAMES)


def _read_data(path):
    _logger.info(f'.. reading data ..')
    with open(path) as f:
        lines = f.readlines()
    sentences = [[]]
    sentence_annotations = [{}]
    for line in lines:
        if line.startswith(_META_MARKER):
            k, v = map(str.strip, line[line.find(_META_MARKER) + 1:].strip().split('=', 1))
            sentence_annotations[-1][k] = v
            continue
        l = line.strip()
        if not l:
            sentences.append([])
            sentence_annotations.append({})
        else:
            try:
                sentences[-1].append(_Token(*l.split('\t')[:8]))
            except TypeError as e:                
                _logger.error(f'Line {line.strip()} cannot be imported (not enough or too many values).')
                raise ValueError(f'Invalid line: "{",".join(l)}"')
    return sentences, sentence_annotations


def _map_entry(u, doc_path, index, entry, text_name=None, anno_qname=None):
    id_ = map_token(u, doc_path, index, text_name, entry.form)
    ns = anno_qname if anno_qname is not None else ('' if text_name is None else text_name)
    for field_name in _FIELD_NAMES[2:5]:
        val = getattr(entry, field_name)
        if val is not None and val.strip() != _NONE:
            map_token_annotation(u, id_, ns, field_name, val)
    if entry.feats is not None and entry.feats.strip() != _NONE:
        features = entry.feats.strip().split(_FEAT_SEP)
        for kv in features:
            k, v = kv.split('=')
            map_token_annotation(u, id_, ns, k.strip(), v.strip())
    if entry.deprel is not None and entry.deprel.strip() != _NONE:
        val = entry.deprel.strip()
        map_token_annotation(u, id_, ns, _FUNC, val)
    return id_, entry.head, entry.deprel


def _map_conll_document(u,
                        path, 
                        internal_path, 
                        text_name=None,
                        anno_qname=None,
                        skip_named_ordering=None):
    sentences, annotations = _read_data(path)
    doc_path = internal_path
    add_subnode(u, doc_path)
    tok_count = 1
    all_nodes = []
    for s_id, (s, a) in enumerate(zip(sentences, annotations), 1):
        nodes = [None]
        for i, tok in enumerate(s, tok_count):
            nodes.append(_map_entry(u, doc_path, i, tok, text_name, anno_qname=anno_qname))
            tok_count += 1
        for node_id, head, deprel in nodes[1:]:
            if not head.isnumeric():
                continue
            h_index = int(head)
            if h_index:
                head_node = nodes[h_index][0]
                add_pointing_relation(u, head_node, node_id, _TYPE_DEP, '', _ANNO_NAME_DEPREL, deprel)        
        span_id = map_annotation(u, doc_path, s_id, '' if text_name is None else text_name, _ANNO_NAME_S, str(s_id), *[n_id for n_id, _, _ in nodes[1:]])
        for k, v in a.items():
            u.add_node_label(span_id, '' if text_name is None else text_name, k, v)
        all_nodes.extend([id_ for id_, _, _ in nodes[1:]])
    add_order_relations(u, all_nodes)
    if not skip_named_ordering and text_name:
        add_order_relations(u, all_nodes, text_name)


def start_import(path, **properties):
    """
    Import all conll documents in the given directory.
    >>> type(start_import('test/conll/importer')).__name__
    'GraphUpdate'
    """
    safe_props = defaultdict(type(None), properties)
    skip_named_ordering = PROPERTY_SKIP_NAMED_ORDERING in safe_props \
        and safe_props[PROPERTY_SKIP_NAMED_ORDERING].lower().strip() == 'true'
    anno_qname = safe_props[PROPERTY_ANNO_NS]
    u = GraphUpdate()
    for path, internal_path in path_structure(u, path, _FILE_ENDINGS):        
        _logger.info(f'Starting {path} ...')
        _map_conll_document(u,
                            path, 
                            internal_path,
                            text_name=safe_props[PROPERTY_TEXT_NAME],
                            anno_qname=anno_qname,
                            skip_named_ordering=skip_named_ordering)
    return u
