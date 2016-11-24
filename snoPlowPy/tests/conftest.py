import pytest


@pytest.fixture
def remote_f():
    return ('https://raw.githubusercontent.com/weng-lab/' +
            'SnoPlowPy/master/snoPlowPy/tests/data/a')
