from insights.formats.text import HumanReadableFormat
import pytest

from ccx_messaging.engines.dvo_extractor_engine import DVOExtractorEngine


def test_init():
    """Test the DVOExtractorEngine constructor."""
    e = DVOExtractorEngine(HumanReadableFormat)

    # just basic check
    assert e is not None

def test_process_no_extract():
    """Basic test for DVOExtractorEngine."""
    e = DVOExtractorEngine(HumanReadableFormat)
    e.watchers = []
    e.extract_tmp_dir = "not-exist"

    broker = None
    path = "not-exist"

    with pytest.raises(Exception):
        e.process(broker, path)


def test_process_extract_wrong_data():
    """Basic test for DVOExtractorEngine."""
    e = DVOExtractorEngine(HumanReadableFormat)
    e.watchers = []
    e.extract_tmp_dir = ""

    broker = None
    path = "test/wrong_data.tar"

    result = e.process(broker, path)
    assert result is None


def test_process_extract_correct_data():
    """Basic test for DVOExtractorEngine."""
    e = DVOExtractorEngine(HumanReadableFormat)
    e.watchers = []
    e.extract_tmp_dir = ""

    broker = None
    path = "test/correct_data.tar"

    result = e.process(broker, path)
    assert result is not None
