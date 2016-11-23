import pytest


@pytest.fixture
def remote_f():
    return ('https://raw.githubusercontent.com/kepbod/' +
            'SnoPlowPy/master/snoPlowPy/tests/data/a')
