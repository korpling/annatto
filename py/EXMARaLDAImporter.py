from glob import iglob
from graphannis.graph import GraphUpdate
from py.graphupdate_util import *
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
# logger
_logger = logging.getLogger(__name__)
_handler = logging.FileHandler('exmaralda-importer.log')
_handler.setLevel(logging.INFO)
_logger.setLevel(logging.INFO)
_logger.addHandler(_handler)


class EXMARaLDAImport(object):
    def __init__(self, path, internal_path, graph_update) -> None:
        self._xml = ElementTree.parse(path)
        self._source_dir = os.path.dirname(path)
        self._path = internal_path
        self._u = graph_update
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
        if not os.path.isabs(referenced_file):
            referenced_file = os.path.join(self._source_dir, referenced_file)
        if not os.path.exists(referenced_file):
            _logger.error(f'Cannot find referenced media file {referenced_file}.')
            return
        u = self._u
        file_name = os.path.basename(referenced_file)
        audio_path = os.path.join(os.path.dirname(self.path), file_name)
        map_audio_source(u, audio_path, self._path)
        self._media_node = audio_path

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
                id_ = map_token(self._u, self._path, token_count, category, text_value, start, end)
                self._spk2tok[speaker][id_] = (start, end)
            add_order_relations(self._u, sorted(self._spk2tok[speaker], key=lambda e: self._spk2tok[speaker][e]), category)

    def _map_annotations(self):
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
                map_annotation(self._u, self._path, span_count, speaker, category, value, *[id_ for _, _, id_ in covered_tokens])

    def _read_timeline(self):
        self._timeline = {tli.attrib[_ATTR_ID]: float(tli.attrib[_ATTR_TIME]) for tli in self._xml.findall(f'.//{_TAG_TLI}[@{_ATTR_TIME}]')}

    def map(self):
        self._read_timeline()
        self._map_audio_source()
        self._map_tokenizations()
        self._map_annotations() 


def start_import(path):
    try:
        _logger.info('------------------------------------------------')
        u = GraphUpdate()
        path = os.path.normpath(path)
        corpus_root = os.path.basename(path)
        u.add_node(corpus_root, node_type=ANNIS_CORPUS)
        _logger.info(f'Starting corpus path {path}')
        for file_path in iglob(f'{path}/**/**exb', recursive=True):
            extra_path = os.path.splitext(file_path[len(path) + 1:])[0]
            _logger.info(f'Reading {file_path} which is {extra_path}')
            segments = []
            root, seg = os.path.split(extra_path)
            _logger.info(f'Initial segments {root} and {seg}')
            while root:
                segments.append(seg)
                root, seg = os.path.split(root)
            prev = corpus_root
            for seg in reversed(segments):
                id_ = os.path.join(prev, seg)
                u.add_node(id_, node_type=ANNIS_CORPUS)
                _logger.info(f'Adding node {id_} as part of {prev}')
                u.add_edge(prev, id_, ANNIS_NS, ANNIS_PART_OF, '')
                prev = id_
            import_ = EXMARaLDAImport(file_path, extra_path, u)
            import_.map()
        return u
    except KeyboardInterrupt:
        exit(1)
