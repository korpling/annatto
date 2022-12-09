from glob import iglob
from graphannis.graph import GraphUpdate
import logging
import os
import sys
from xml.etree import ElementTree

from graphupdate_util import *

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
_handler = logging.StreamHandler(stream=sys.stdout)
_handler.setLevel(logging.DEBUG)
_logger.setLevel(logging.DEBUG)
_logger.addHandler(_handler)

_FILE_ENDINGS = ('.exb', '.xml')
PROP_TEXT_ORDER = 'text_order'


class EXMARaLDAImport(object):
    def __init__(self, graph_update, path, internal_path) -> None:
        self._xml = ElementTree.parse(path)
        self._source_dir = os.path.dirname(path)
        self._path = internal_path
        self._u = graph_update
        self._media_node = None
        self._spk2tok = {}
        self._timeline = None
        self._span_count = 0
        self._token_count = 0

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

    def _map_tokenizations(self, text_order=None):
        xml = self._xml
        tl = self._timeline
        token_tiers = xml.findall(f'.//{_TAG_TIER}[@{_ATTR_TYPE}="{_TYPE_TOK}"]')
        if text_order is not None:
            token_tiers.sort(key=lambda t: text_order.index(t.attrib[_ATTR_CATEGORY]))
        time_values = sorted(set(tl[k] for tier in token_tiers for e in tier for k in (e.attrib[_ATTR_START], e.attrib[_ATTR_END])))
        empty_toks = [(time_values[i - 1], map_token(self._u, self._path, i, '', ' ', time_values[i - 1], time_values[i])) for i in range(1, len(time_values))]
        add_order_relations(u, [t for _, t in empty_toks])
        _logger.debug(f'Created {len(empty_toks)} empty tokens and their order relations')
        for tier in token_tiers:            
            category = tier.attrib[_ATTR_CATEGORY]
            _logger.debug(f'Importing token tier {category}')
            try:
                speaker = tier.attrib[_ATTR_SPEAKER]
            except KeyError:
                raise ValueError(f'Tier {category} has no speaker assigned.')
            if speaker in self._spk2tok:
                raise ValueError(f'Speaker {speaker} has more than one tokenization.')
            tokens = [(tl[e.attrib[_ATTR_START]], tl[e.attrib[_ATTR_END]], e.text) for e in tier.findall(f'./{_TAG_EVENT}')]
            self._spk2tok[speaker] = {}
            for start, end, text_value in sorted(tokens):
                self._span_count += 1
                id_ = map_token_as_span(self._u, self._path, self._span_count, category, text_value, start, end, empty_toks)
                self._spk2tok[speaker][id_] = (start, end)            
            add_order_relations(self._u, sorted(self._spk2tok[speaker], key=lambda e: self._spk2tok[speaker][e]), category)        
            _logger.debug(f'Created order relations for {len(self._speak2tok[speaker])} tokens')

    def _map_annotations(self):
        xml = self._xml
        anno_tiers = xml.findall(f'.//{_TAG_TIER}[@{_ATTR_TYPE}="{_TYPE_ANNOTATION}"]')
        tl = self._timeline
        for tier in anno_tiers:            
            speaker = tier.attrib[_ATTR_SPEAKER]
            category = tier.attrib[_ATTR_CATEGORY]            
            tokens = sorted([(start, end, tok_id) for tok_id, (start, end) in self._spk2tok[speaker].items()])
            _logger.debug(f'Mapping annotations for tier {speaker}::{category} using {len(tokens)} tokens')
            for event in sorted(tier.findall(f'./{_TAG_EVENT}'), key=lambda e: tl[e.attrib[_ATTR_START]]):
                start = tl[event.attrib[_ATTR_START]]
                end = tl[event.attrib[_ATTR_END]]
                if event.text is None:
                    continue
                value = event.text.strip()
                covered_tokens = filter(lambda t: t[0] >= start and t[1] <= end, tokens)
                self._span_count += 1
                map_annotation(self._u, self._path, self._span_count, speaker, category, value, *[id_ for _, _, id_ in covered_tokens])

    def _read_timeline(self):
        self._timeline = {tli.attrib[_ATTR_ID]: float(tli.attrib[_ATTR_TIME]) for tli in self._xml.findall(f'.//{_TAG_TLI}[@{_ATTR_TIME}]')}

    def map(self, text_order=None):
        self._read_timeline()
        self._map_audio_source()
        self._map_tokenizations(text_order=text_order)
        self._map_annotations() 


def start_import(path, **properties):
    """
    Import all conll documents in the given directory.
    >>> type(start_import('test/exmaralda/importer')).__name__
    'GraphUpdate'
    """
    try:
        _logger.info('------------------------------------------------')
        u = GraphUpdate()
        _logger.info(f'Starting corpus path {path}')
        text_order = [t.strip() for t in properties[PROP_TEXT_ORDER].split(';')] \
                      if PROP_TEXT_ORDER in properties else None
        for path, internal_path in path_structure(u, path, _FILE_ENDINGS, logger=_logger):
            _logger.info(f'Reading {path} which is {internal_path}')
            import_ = EXMARaLDAImport(u, path, internal_path)
            import_.map(text_order=text_order)
        return u
    except KeyboardInterrupt:
        _logger.error(f'Imports cancelled by user (keyboard interrupt).')
        exit(1)
