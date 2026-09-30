"""Accept navigation literals as data before trusted website tooling imports them."""

import ast
import json
from pathlib import PurePosixPath
import re

NAV = 'website/src/lib/config/nav.ts'
REDIRECTS = 'website/redirects.json'
SECTIONS = {'getting-started', 'guides', 'concepts', 'reference', 'contributing'}
AUXILIARY = {NAV, REDIRECTS}
TOKEN = re.compile(r'''\s+|/\*.*?\*/|//[^\r\n]*|'(?:[^'\\\r\n]|\\.)*'|"(?:[^"\\\r\n]|\\.)*"|[A-Za-z_][A-Za-z_0-9]*|[][{}:,]''', re.S)


def page_path(value):
    return (isinstance(value, str) and value.isprintable() and '\\' not in value
            and value.endswith('.md') and not value.startswith('/')
            and '..' not in PurePosixPath(value).parts and str(PurePosixPath(value)) == value)


def navigation(text):
    """Parse only a fixed type import followed by an array of object literals.

    Never evaluate TypeScript. The token grammar excludes calls, spreads,
    computed keys, getters, templates, assignments and trailing statements.
    """
    if '\u2028' in text or '\u2029' in text:
        raise ValueError('Unicode line separators are not allowed in navigation')
    wrapper = re.fullmatch(
        r"\s*import type \{ NavSection \} from '\$lib/docs/types';\s*"
        r"(?:/\*(?:[^*]|\*(?!/))*\*/\s*)?export const nav: NavSection\[\] = (\[.*\]);\s*", text, re.S)
    if not wrapper:
        raise ValueError('Navigation must retain its fixed import and literal export')
    source, tokens, offset = wrapper[1], [], 0
    while offset < len(source):
        match = TOKEN.match(source, offset)
        if not match:
            raise ValueError('Executable or unsupported navigation syntax')
        token = match[0]
        offset = match.end()
        if not token.isspace() and not token.startswith(('/*', '//')):
            tokens.append(token)
    converted = []
    for index, token in enumerate(tokens):
        following = tokens[index + 1] if index + 1 < len(tokens) else ''
        if token.startswith(('"', "'")):
            converted.append(json.dumps(ast.literal_eval(token)))
        elif re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', token):
            if following == ':':
                converted.append(json.dumps(token))
            elif token in {'true', 'false', 'null'}:
                converted.append(token)
            else:
                raise ValueError('Navigation values must be literals')
        elif token == ',' and following in {']', '}'}:
            continue
        else:
            converted.append(token)
    data = json.loads(''.join(converted))
    if not isinstance(data, list) or len(data) != len(SECTIONS):
        raise ValueError('Navigation must contain all five sections')
    seen = set()
    for section in data:
        if (not isinstance(section, dict) or set(section) != {'id', 'label', 'groups'}
                or not isinstance(section['id'], str) or section['id'] not in SECTIONS
                or section['id'] in seen or not isinstance(section['label'], str)
                or not isinstance(section['groups'], list)):
            raise ValueError('Invalid navigation section')
        seen.add(section['id'])
        for group in section['groups']:
            if (not isinstance(group, dict) or not {'label', 'pages'} <= set(group)
                    or set(group) - {'label', 'pages', 'preview'}
                    or not (group['label'] is None or isinstance(group['label'], str))
                    or ('preview' in group and type(group['preview']) is not bool)
                    or not isinstance(group['pages'], list) or not all(map(page_path, group['pages']))):
                raise ValueError('Invalid navigation group')
    return data


def validate(path, text):
    if path == NAV:
        navigation(text)
    elif path == REDIRECTS:
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError('Redirects must be an array')
        for row in data:
            if (not isinstance(row, dict) or set(row) != {'old', 'new', 'rewrittenFrom'}
                    or not page_path(row['old']) or not page_path(row['new'])
                    or row['new'].split('/')[0] not in SECTIONS
                    or not (row['rewrittenFrom'] is None or
                            isinstance(row['rewrittenFrom'], str) and
                            re.fullmatch('[0-9a-f]{7,40}', row['rewrittenFrom']))):
                raise ValueError('Invalid redirect entry')
