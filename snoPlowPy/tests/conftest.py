import pytest
import requests
import json
import random
import string
import datetime
from lxml import html


@pytest.fixture
def remote_f():
    return ('https://raw.githubusercontent.com/weng-lab/' +
            'SnoPlowPy/master/snoPlowPy/tests/data/a')


@pytest.fixture(scope='module')
def exp_jsondata_generator():
    return fake_jsondata('experiment', 1)


def fake_jsondata(jId, depth):
    temp = fetch_temp(jId)
    fake_json_dict = {}
    for pid, jsondata in temp.items():
        fake_json_dict[pid] = random_entry(jsondata, pid, depth)
    return fake_json_dict


def random_entry(jsondata, pid, depth):
    entry_type = jsondata['type']
    if entry_type == 'array':
        return []
    else:
        if 'enum' in jsondata:
            return random.choice(jsondata['enum'])
        elif pid.startswith('date'):
            return str(datetime.date.today())
        elif 'linkTo' in jsondata:
            depth += 1
            if depth > 2:
                return None
            else:
                return fake_jsondata(jsondata['linkTo'], depth)
        else:
            return ''.join(random.choice(string.ascii_letters) for i in range(10))


def fetch_temp(jId):
    json_temp_url = 'https://www.encodeproject.org/profiles/%s.json' % jId
    tree = html.fromstring(requests.get(json_temp_url).text)
    json_temp = tree.xpath('//*[@id="content"]/div/section/div/pre/text()')[0]
    return json.loads(json_temp)['properties']
