from collections import defaultdict, namedtuple
from glob import iglob
from graphannis.graph import GraphUpdate
from py.graphupdate_util import *
import os


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
_Token = namedtuple('Token', _FIELD_NAMES)


def _read_data(path):
    with open(path) as f:
        lines = f.readlines()
    sentences = [[]]
    for line in lines:
        l = line.strip().split('\t')
        if not l:
            sentences.append([])
        elif len(l) == 10:
            sentences[-1].append(_Token(*l[:8]))
    return sentences


def _map_entry(u, doc_path, index, entry, text_name=None):
    id_ = map_token(u, doc_path, index, text_name, entry.form)
    ns = '' if text_name is None else text_name
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


def _map_conll_document(path, u, text_name=None):
    sentences = _read_data(path)
    doc_path = os.path.splitext(path)[0]
    add_subnode(u, doc_path)
    tok_count = 1
    for s in sentences:
        nodes = [None]
        for i, tok in enumerate(s, tok_count):
            nodes.append(_map_entry(u, doc_path, i, tok, text_name))
            tok_count += 1
        for node_id, head, deprel in nodes[1:]:
            h_index = int(head)
            if h_index:
                head_node = nodes[h_index][0]
                add_pointing_relation(u, head_node, node_id, _TYPE_DEP, text_name, _ANNO_NAME_DEPREL, deprel)


def start_import(path, **kwargs):
    properties = defaultdict(type(None), kwargs)
    u = GraphUpdate()
    base_dir = os.path.normpath(path)
    corpus_root(u, os.path.basename(base_dir))
    for path in iglob(f'{base_dir}/**/*.conllu', recursive=True):
        _map_conll_document(path, u, text_name=properties['text_name'])
    return u
