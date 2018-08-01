# encoding: utf-8

'''

@author: xupengfei

'''
import arrow


def trim_prefix(s, sub):
    if not s.startswith(sub):
        return s
    return s[len(sub):]


def trim_suffix(s, sub):
    if not s.endswith(sub):
        return s
    return s[:-len(sub)]


identifier_start_quote = '`'
identifier_end_quote = '`'


def quote_identifier(v):
    parts = []
    v_splits = v.split('.')
    for x in v_splits:
        if x == '':
            continue
        x = trim_prefix(x, identifier_start_quote)
        x = trim_suffix(x, identifier_end_quote)
        x = f'{identifier_start_quote}{x}{identifier_end_quote}'
        parts.append(x)
    return '.'.join(parts)


def parse_local_time(v):
    return arrow.get(v).replace(tzinfo=None)
