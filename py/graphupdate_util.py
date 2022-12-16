from collections import defaultdict
import os
import re
from venv import create

ANNIS_CORPUS = 'corpus'
ANNIS_COVERAGE = 'Coverage'
ANNIS_DOMINANCE = 'Dominance'
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


def path_structure(u, root_path, file_endings, logger=None):
    norm_path = os.path.normpath(root_path)
    root_name = os.path.basename(norm_path)
    if logger is not None:
        logger.info(f'Creating corpus root {root_name}')
    corpus_root(u, root_name)
    created_paths = set()
    path_tuples = set()
    for root, _, f_names in os.walk(norm_path):
        for doc_name in filter(lambda fn: os.path.splitext(fn)[1] in file_endings, f_names):
            path = os.path.join(root, doc_name)
            internal_path = os.path.splitext(os.path.join(root_name, path[len(norm_path) + 1:]))[0]
            if internal_path not in created_paths:
                segments = internal_path.split(os.pathsep)
                for n_segments in range(2, len(segments)):
                    inner_node = os.pathsep.join(segments[:n_segments])
                    if inner_node not in created_paths:
                        if logger is not None:
                            logger.info(f'Creating inner node {inner_node}')
                        add_subnode(u, inner_node)
                        created_paths.add(inner_node)
                if logger is not None:
                    logger.info(f'Creating corpus node {internal_path}')
                add_subnode(u, internal_path)
                created_paths.add(internal_path)
                path_tuples.add((path, internal_path))
    return sorted(path_tuples)


def corpus_root(u, root_name):
    u.add_node(root_name, node_type=ANNIS_CORPUS)


def add_subnode(u, path):
    parent = os.path.dirname(path)
    u.add_node(path.replace(os.pathsep, '/'), node_type=ANNIS_CORPUS)
    u.add_edge(path.replace(os.pathsep, '/'), parent.replace(os.pathsep, '/'), ANNIS_NS, ANNIS_PART_OF, '')


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


def map_token_as_span(u, doc_path, id_, text_name, value, start_time, end_time, empty_toks):
    """
    """
    if start_time >= end_time:
        raise ValueError(f'Token {id_} with value {value} in tokenization {text_name} has incorrect time values.')
    ets = [et_id for t, et_id in empty_toks if start_time <= t < end_time]
    span_id = map_annotation(u, doc_path, id_, '', text_name, value, *ets)
    u.add_node_label(span_id, ANNIS_NS, ANNIS_TOK, value)
    return span_id


def map_annotation(u, doc_path, id_, ns, name, value, *targets):
    span_id = f'{doc_path}#sSpan{id_}'
    u.add_node(span_id)
    if name:
        u.add_node_label(span_id, ns, name, value)
    for target in targets:
        coverage(u, [span_id], [target])
    return span_id


def map_hierarchical_annotation(u, doc_path, id_, ns, name, value, *targets, edge_layer=''):
    struct_id = f'{doc_path}#sStruct{id_}'
    u.add_node(struct_id)
    if name:
        u.add_node_label(struct_id, ns, name, value)
    for target in targets:
        dominance(u, [struct_id], [target], layer=edge_layer)
    return struct_id


def map_token_annotation(u, target_uri, ns, name, value):
    u.add_node_label(target_uri, ns, name, value)


def add_order_relations(u, node_ids, order_name=None):
    for i in range(1, len(node_ids)):
        u.add_edge(node_ids[i - 1], node_ids[i], ANNIS_NS, ANNIS_ORDERING, order_name if order_name else '')        


def add_pointing_relation(u, source, target, type_, anno_ns=None, anno_name=None, anno_val=None, component_layer=''):
    u.add_edge(source, target, component_layer, ANNIS_POINTING_REL, type_)
    if anno_name is not None and anno_val is not None:
        u.add_edge_label(source, target, component_layer, ANNIS_POINTING_REL, type_, '' if anno_ns is None else anno_ns, anno_name, anno_val)


def edges(u, source_nodes, target_nodes, component_type, layer=''):
    for src in source_nodes:
        for tgt in target_nodes:
            u.add_edge(src, tgt, ANNIS_NS, component_type, layer)


def coverage(u, source_nodes, target_nodes):
    edges(u, source_nodes, target_nodes, ANNIS_COVERAGE)


def dominance(u, source_nodes, target_nodes, layer=''):
    edges(u, source_nodes, target_nodes, ANNIS_DOMINANCE, layer)
