"""Tests for ccx_messaging.utils.sliced_template module."""

import uuid

import pytest

from ccx_messaging.utils.sliced_template import SlicedTemplate


def test_standard_substitutions():
    """Check a standard substitution."""
    template = SlicedTemplate("$cluster_name")
    cluster_id = str(uuid.uuid4())

    assert template.safe_substitute(cluster_name=cluster_id) == cluster_id
    assert template.safe_substitute({"cluster_name": cluster_id}) == cluster_id
    assert template.safe_substitute({"other": "ignored"}, cluster_name=cluster_id) == cluster_id
    assert template.substitute(cluster_name=cluster_id) == cluster_id
    assert template.substitute({"cluster_name": cluster_id}) == cluster_id
    assert template.substitute({"other": "ignored"}, cluster_name=cluster_id) == cluster_id


def test_failed_substitutions():
    """Check some failing standard substitutions."""
    template = SlicedTemplate("$cluster_name")

    assert template.safe_substitute(other="ignored") == "$cluster_name"
    assert template.safe_substitute({"other": "ignored"}) == "$cluster_name"

    with pytest.raises(KeyError):
        template.substitute(other="ignored")
        template.substitute({"other": "ignored"})


def test_sliced_substitution():
    """Check a substitution with slicing."""
    template = SlicedTemplate("$cluster_name[0:20]")
    cluster_id = str(uuid.uuid4())

    expected = cluster_id[:20]

    assert template.safe_substitute(cluster_name=cluster_id) == expected
    assert template.safe_substitute({"cluster_name": cluster_id}) == expected
    assert template.substitute(cluster_name=cluster_id) == expected
    assert template.substitute({"cluster_name": cluster_id}) == expected


def test_failed_sliced_substitutions():
    """Check some failing standard substitutions."""
    template = SlicedTemplate("$cluster_name[0:20]")

    assert template.safe_substitute(other="ignored") == "$cluster_name[0:20]"
    assert template.safe_substitute({"other": "ignored"}) == "$cluster_name[0:20]"

    with pytest.raises(KeyError):
        template.substitute(other="ignored")
        template.substitute({"other": "ignored"})


def test_element_substitution():
    """Check a substitution with slicing."""
    template = SlicedTemplate("$cluster_name[15]")
    cluster_id = str(uuid.uuid4())

    expected = cluster_id[15]

    assert template.safe_substitute(cluster_name=cluster_id) == expected
    assert template.safe_substitute({"cluster_name": cluster_id}) == expected
    assert template.substitute(cluster_name=cluster_id) == expected
    assert template.substitute({"cluster_name": cluster_id}) == expected


def test_get_identifiers():
    """Check that get_identifiers find out real identifiers."""
    template = SlicedTemplate("$cluster_name[0:20]")
    assert template.get_identifiers() == ["cluster_name"]
