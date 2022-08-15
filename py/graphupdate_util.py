import os

ANNIS_CORPUS = 'corpus'
ANNIS_COVERAGE = 'Coverage'
ANNIS_POINTING_REL = 'Pointing'
ANNIS_FILE = 'file'
ANNIS_NS = 'annis'
ANNIS_NODE_TYPE = 'node_type'
ANNIS_NODE_TYPE_FILE = 'file'
ANNIS_ORDERING = 'Ordering'
ANNIS_PART_OF = 'PartOf'
ANNIS_TIME = 'time'
ANNIS_TOK = 'tok'
ANNIS_TOK_WHITE_SPACE_AFTER = 'tok-whitespace-after'


def corpus_root(u, root_name):
    u.add_node(root_name, node_type=ANNIS_CORPUS)


def add_subnode(u, path):
    parent = os.path.dirname(path)
    u.add_node(path, node_type=ANNIS_CORPUS)
    u.add_edge(path, parent, ANNIS_NS, ANNIS_PART_OF, '')


def map_audio_source(u, audio_path, doc_path):
    u.add_node(audio_path)
    u.add_node_label(audio_path, ANNIS_NS, ANNIS_NODE_TYPE, ANNIS_NODE_TYPE_FILE)
    u.add_node_label(audio_path, ANNIS_NS, ANNIS_FILE, audio_path)
    u.add_edge(audio_path, doc_path, ANNIS_NS, ANNIS_PART_OF, '')
    return audio_path


def map_token(u, doc_path, id_, text_name, value, start_time=None, end_time=None):        
    tok_id = f'{doc_path}#t{id_}'
    u.add_node(tok_id)
    u.add_node_label(tok_id, ANNIS_NS, ANNIS_TOK, value)
    if text_name is not None and text_name.strip():
        u.add_node_label(tok_id, '', text_name, value)
    u.add_node_label(tok_id, ANNIS_NS, ANNIS_TOK_WHITE_SPACE_AFTER, ' ')
    u.add_edge(tok_id, doc_path, ANNIS_NS, ANNIS_PART_OF, '')
    if start_time is not None and end_time is not None:
        if start_time >= end_time:
            raise ValueError(f'Token {id_} with value {value} in tokenization {text_name} has incorrect time values.')
        u.add_node_label(tok_id, ANNIS_NS, ANNIS_TIME, f'{start_time}-{end_time}')
    return tok_id


def map_annotation(u, doc_path, id_, ns, name, value, *targets):
    span_id = f'{doc_path}#sSpan{id_}'
    u.add_node(span_id)
    u.add_node_label(span_id, ns, name, value)
    for target in targets:
        u.add_edge(span_id, target, ANNIS_NS, ANNIS_COVERAGE, '')
    return span_id


def map_token_annotation(u, target_uri, ns, name, value):
    u.add_node_label(target_uri, ns, name, value)


def add_order_relations(u, node_ids, order_name=None):
    for i in range(1, len(node_ids)):
        if order_name is not None:
            u.add_edge(node_ids[i - 1], node_ids[i], ANNIS_NS, ANNIS_ORDERING, order_name)
        u.add_edge(node_ids[i - 1], node_ids[i], ANNIS_NS, ANNIS_ORDERING, '')


def add_pointing_relation(u, source, target, type_, anno_ns=None, anno_name=None, anno_val=None):
    u.add_edge(source, target, '', ANNIS_POINTING_REL, type_)
    if anno_name is not None and anno_val is not None:
        u.add_edge_label(source, target, '', ANNIS_POINTING_REL, type_, '' if anno_ns is None else anno_ns, anno_name, anno_val)

