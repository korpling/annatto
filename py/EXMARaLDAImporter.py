from graphannis.graph import GraphUpdate
import logging
import os
from xml.etree import ElementTree


# EXMARALDA
_TYPE_TOK = 't'
_TYPE_ANNOTATION = 'a'
_TAG_EVENT = 'event'
_TAG_REFERENCED_FILE = 'referenced-file'
_TAG_TIER = 'tier'
_TAG_TLI = 'tli'
_ATTR_END = 'end'
_ATTR_ID = 'id'
_ATTR_CATEGORY = 'category'
_ATTR_SPEAKER = 'speaker'
_ATTR_START = 'start'
_ATTR_TIME = 'time'
_ATTR_TYPE = 'type'
_ATTR_URL = 'url'
# ANNIS
_ANNIS_COVERAGE = 'Coverage'
_ANNIS_FILE = 'file'
_ANNIS_NS = 'annis'
_ANNIS_NODE_TYPE = 'node_type'
_ANNIS_NODE_TYPE_FILE = 'file'
_ANNIS_ORDERING = 'Ordering'
_ANNIS_PART_OF = 'PartOf'
_ANNIS_TIME = 'time'
_ANNIS_TOK = 'tok'
_ANNIS_TOK_WHITE_SPACE_AFTER = 'tok-whitespace-after'
# logger
_logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_handler.setLevel(logging.INFO)
_logger.setLevel(logging.INFO)
_logger.addHandler(_handler)


class EXMARaLDAImport(object):
    def __init__(self, path) -> None:
        self._xml = ElementTree.parse(path)
        self._path = os.path.splitext(path)[0]
        self._u = GraphUpdate()
        self._media_node = None
        self._spk2tok = {}
        self._timeline = None

    @property
    def name(self):
        return os.path.basename(self._path)

    @property
    def path(self):
        return self._path

    @property
    def u(self):
        return self._u

    def _map_audio_source(self):
        if self._media_node is not None:
            return
        referenced_files = self._xml.findall(f'.//{_TAG_REFERENCED_FILE}[@{_ATTR_URL}]')
        if not referenced_files:
            return
        if len(referenced_files) > 1:
            raise ValueError(f'More than one referenced file in {self.name}.')
        referenced_file = referenced_files[0].attrib[_ATTR_URL]
        u = self._u
        file_name = os.path.basename(referenced_file)
        corpus_path = os.path.join(os.path.dirname(self.path), file_name)
        u.add_node(corpus_path)
        u.add_node_label(corpus_path, _ANNIS_NS, _ANNIS_NODE_TYPE, _ANNIS_NODE_TYPE_FILE)
        u.add_node_label(corpus_path, _ANNIS_NS, _ANNIS_FILE, corpus_path)
        u.add_node_edge(corpus_path, self._path, _ANNIS_NS, _ANNIS_PART_OF, '')
        self._media_node = corpus_path

    def _map_tokenizations(self):
        xml = self._xml
        tl = self._timeline
        token_tiers = xml.findall(f'.//{_TAG_TIER}[@{_ATTR_TYPE}="{_TYPE_TOK}"]')
        token_count = 0
        for tier in token_tiers:
            category = tier.attrib[_ATTR_CATEGORY]
            try:
                speaker = tier.attrib[_ATTR_SPEAKER]
            except KeyError:
                raise ValueError(f'Tier {category} has no speaker assigned.')
            if speaker in self._spk2tok:
                raise ValueError(f'Speaker {speaker} has more than one tokenization.')
            tokens = [(tl[e.attrib[_ATTR_START]], tl[e.attrib[_ATTR_END]], e.text) for e in tier.findall(f'./{_TAG_EVENT}')]
            self._spk2tok[speaker] = {}
            for start, end, text_value in sorted(tokens):
                token_count += 1
                id_ = self._map_token(token_count, category, text_value, start, end)
                self._spk2tok[speaker][id_] = (start, end)
            self._add_order_relations(sorted(self._spk2tok[speaker], key=lambda e: self._spk2tok[speaker][e]), category)

    def _map_token(self, id_, text_name, value, start_time=None, end_time=None):        
        u = self._u
        tok_id = f'{self.path}#t{id_}'
        u.add_node(tok_id)
        u.add_node_label(tok_id, _ANNIS_NS, _ANNIS_TOK, value)
        u.add_node_label(tok_id, '', text_name, value)
        u.add_node_label(tok_id, _ANNIS_NS, _ANNIS_TOK_WHITE_SPACE_AFTER, ' ')
        u.add_edge(tok_id, self._path, _ANNIS_NS, _ANNIS_PART_OF, '')
        if start_time is not None and end_time is not None:
            if start_time >= end_time:
                raise ValueError(f'Token {id_} with value {value} in tokenization {text_name} has incorrect time values.')
            u.add_node_label(tok_id, _ANNIS_NS, _ANNIS_TIME, f'{start_time}-{end_time}')
        return tok_id

    def _add_order_relations(self, node_ids, order_name):
        u = self._u
        for i in range(1, len(node_ids)):
            u.add_edge(node_ids[i - 1], node_ids[i], _ANNIS_NS, _ANNIS_ORDERING, order_name)
            u.add_edge(node_ids[i - 1], node_ids[i], _ANNIS_NS, _ANNIS_ORDERING, '')

    def _map_annotations(self):
        u = self._u
        xml = self._xml
        anno_tiers = xml.findall(f'.//{_TAG_TIER}[@{_ATTR_TYPE}="{_TYPE_ANNOTATION}"]')
        tl = self._timeline
        span_count = 0
        for tier in anno_tiers:
            speaker = tier.attrib[_ATTR_SPEAKER]
            category = tier.attrib[_ATTR_CATEGORY]
            tokens = sorted([(start, end, tok_id) for tok_id, (start, end) in self._spk2tok[speaker].items()])
            for event in sorted(tier.findall(f'./{_TAG_EVENT}'), key=lambda e: tl[e.attrib[_ATTR_START]]):
                start = tl[event.attrib[_ATTR_START]]
                end = tl[event.attrib[_ATTR_END]]
                if event.text is None:
                    continue
                value = event.text.strip()
                covered_tokens = filter(lambda t: t[0] >= start and t[1] <= end, tokens)
                span_count += 1
                self._map_annotation(span_count, speaker, category, value, *[id_ for _, _, id_ in covered_tokens])

    def _map_annotation(self, id_, ns, name, value, *targets):
        u = self._u
        span_id = f'{self.path}#sSpan{id_}'
        u.add_node(span_id)
        u.add_node_label(span_id, ns, name, value)
        for target in targets:
            u.add_edge(span_id, target, _ANNIS_NS, _ANNIS_COVERAGE, '')


    def _map_base_structure(self):
        u = self._u
        segments = []
        root, seg = os.path.split(self.path)
        while root:
            segments.append(seg)
            root, seg = os.path.split(root)
        segments.append(seg)
        for seg in reversed(segments):
            u.add_node(seg)

    def _read_timeline(self):
        self._timeline = {tli.attrib[_ATTR_ID]: float(tli.attrib[_ATTR_TIME]) for tli in self._xml.findall(f'.//{_TAG_TLI}[@{_ATTR_TIME}]')}

    def map(self):
        self._read_timeline()
        self._map_base_structure()
        self._map_audio_source()
        self._map_tokenizations()
        self._map_annotations() 


def start_import(path):
    import_ = EXMARaLDAImport(path)
    import_.map()
    return import_.u
