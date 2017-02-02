import pytest


@pytest.fixture
def coordinator_xml_with_controls():
    with open('tests/data/coordinator-with-controls.xml', 'r') as fh:
        return fh.read()
