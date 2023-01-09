from collections import defaultdict
from graphannis.graph import GraphUpdate
from graphupdate_util import *
from itertools import chain
import logging


_FILE_ENDINGS = ('textgrid', 'TextGrid', 'textGrid')

_FILE_TYPE_SHORT = 'ooTextFile short'
_FILE_TYPE_LONG = 'ooTextFile'
_TIER_CLASS_INTERVAL = 'IntervalTier'
_TIER_CLASS_POINT = 'PointTier'

_PROP_TIER_GROUPS = 'tier_groups'
_PROP_FORCE_MULTI_TOK = 'force_multi_tok'

# logger
_logger = logging.getLogger(__name__)
_handler = logging.FileHandler('conll-importer.log')
_handler.setLevel(logging.INFO)
_stream = logging.StreamHandler(stream=sys.stdout)
_stream.setLevel(logging.INFO)
_logger.setLevel(logging.INFO)
_logger.addHandler(_handler)
_logger.addHandler(_stream)


def map_document(u, file_path, corpus_doc_path, tier_map, force_multitok=False):
    with open(file_path) as f:
        data = f.readlines()
    if not data:
        return
    header = data[0]
    file_type = header[header.find('"') + 1:header.rfind('"')]
    tier_names = {[k] + list(v) for k, v in tier_map.items()}
    tiers_and_values = process_data(u, data, tier_names, short=file_type == _FILE_TYPE_SHORT)
    is_multi_tok = len(tier_map) > 1 or force_multitok
    tok_dict = {}
    if is_multi_tok:
        total_time_values = sorted(set(chain(*((t0, t1) for tok_name in tier_names for t0, t1, _ in tiers_and_values[tok_name]))))        
        for i in range(len(total_time_values)):
            start, end = total_time_values[i:i + 2]
            tok_dict[(start, end)] = map_token(u, corpus_doc_path, i + 1, '', ' ', start, end)
        add_order_relations(u, [id_ for (s, e), id_ in sorted(tok_dict.items(), key=lambda e: e[0][0])], '')
    tc = len(tok_dict) if is_multi_tok else 0
    spc = 0
    for tok_tier, dependent_tiers in tier_map.items():
        for start, end, value in tiers_and_values[tok_tier]:
            tok_dict[(start, end, tok_tier)] = map_token(u, corpus_doc_path, tc, tok_tier, value, start, end)
            tc += 1
            if is_multi_tok:
                overlapped = [id_ for k, id_ in tok_dict.items() if len(k) == 2 and start <= k[0] and end >= k[1]]
                coverage(u, [tok_dict[(start, end, tok_tier)]], overlapped)
        all_tokens = [id_ for (s, e, name), id_ in sorted(tok_dict.items(), key=lambda e: e[0][0]) if name == tok_tier]
        add_order_relations(u, all_tokens, tok_tier)
        span_dict = {}
        for tier_name in dependent_tiers:
            for start, end, value in tiers_and_values[tier_name]:
                if (start, end) not in span_dict:
                    spc += 1
                    overlapped = [id_ for k, id_ in tok_dict.items() if len(k) == 3 and k[2] == tok_tier and start <= k[0] and end >= k[1]]
                    span_dict[(start, end)] = map_annotation(u, corpus_doc_path, spc, tok_tier, tier_name, value, *overlapped)
                else:
                    u.add_node_label(span_dict[(start, end)], tok_tier, tier_name, value)


def process_data(u, data, tier_names, short=False):
    resolver = resolve_short if short else resolve_long
    gathered = []
    size = 0
    tier_data = defaultdict(list)
    for line in data[9:]:
        l = line.strip()
        if size == 0:  # reading tier header
            if len(gathered) < 5:
                gathered.append(resolver(l))
            else:
                clz, name, _, _, size = gathered
                gathered.clear()
        else:  # reading items
            if not short and l.startswith('intervals'):
                continue  # skip one for explicit format
            if len(gathered) < (2 + clz == _TIER_CLASS_INTERVAL):
                gathered.append(resolver(l))
            else:
                tier_data[name].append(tuple(gathered))
                gathered.clear()
                size -= 1
    return tier_data


def resolve_short(value):    
    if value.startswith('"'):
        return value[1:-1]
    elif '.' in value:
        return float(value)
    else:
        return int(value)


def resolve_long(value):
    bare_value = value.split(' = ', 1)[1]
    return resolve_short(bare_value)


def parse_tier_map(value):
    return ''


def start_import(path, **properties):
    u = GraphUpdate()
    try:
        tier_config = parse_tier_map(properties.pop([_PROP_TIER_GROUPS]))
    except KeyError:
        _logger.exception(f'No tier mapping configurated. Cannot proceed.')
    for path, internal_path in path_structure(u, path, _FILE_ENDINGS):
        map_document(u, path, internal_path, tier_config, **properties)
    return u
