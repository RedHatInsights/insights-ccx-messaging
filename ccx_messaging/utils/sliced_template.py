"""A string template with slice substitutions."""

from __future__ import annotations

import re
import string
from collections import ChainMap
from dataclasses import dataclass


_sentinel_map = {}


@dataclass
class SliceSelector:
    """Representation of a slice substitution."""

    name: str
    initial: int
    final: int
    element: int

    @classmethod
    def from_match(cls, match_: re.Match) -> SliceSelector:
        """Create a SliceSelector from a pattern match."""
        element = match_.group("element")
        element = int(element) if element else None
        initial = match_.group("initial")
        initial = int(initial) if initial else None
        final = match_.group("final")
        final = int(final) if final else None
        var_id = match_.group("identifier")

        return SliceSelector(
            var_id,
            initial,
            final,
            element,
        )


class SlicedTemplate(string.Template):
    """A template class for support $-substitutions with element slicing."""

    idpattern = r"(?a:[_a-z][_a-z0-9]*((\[(-?\d+)?:(-?\d+)?\])|(\[-?\d+\]))?)"
    idpattern_with_slice = re.compile(
        r"(?P<identifier>[_a-z][_a-z0-9]*)"
        r"(?:(?:\[(?P<initial>-?\d+)?:(?P<final>-?\d+)?\])|"
        r"(?:\[(?P<element>-?\d+)\]))"
    )

    def __init__(self, template):
        """Initialise the SliceTemplate object."""
        super().__init__(template)
        self.slice_selectors = {}

        for identifier in super().get_identifiers():
            match_ = SlicedTemplate.idpattern_with_slice.match(identifier)
            if not match_:
                continue

            self.slice_selectors[identifier] = SliceSelector.from_match(match_)

    def substitute(self, mapping=_sentinel_map, /, **kws) -> str:
        """Perform the substitution using the mapping dictionary and named arguments.

        It fails when a template identifier is not found.
        """
        if mapping is _sentinel_map:
            mapping = kws

        elif kws:
            mapping = ChainMap(mapping, kws)

        for identifier, selector in self.slice_selectors.items():
            if selector.element:
                slice_value = mapping[selector.name][selector.element]
            else:
                slice_value = mapping[selector.name][selector.initial : selector.final]

            mapping[identifier] = slice_value

        return super().substitute(mapping)

    def safe_substitute(self, mapping=_sentinel_map, /, **kws) -> str:
        """Perform the substitution using the mapping dictionary and named arguments.

        It will ignore substitutions if the referenced identifier is not found.
        """
        if mapping is _sentinel_map:
            mapping = kws

        elif kws:
            mapping = ChainMap(mapping, kws)

        for identifier, selector in self.slice_selectors.items():
            if selector.name not in mapping:
                continue

            if selector.element:
                slice_value = mapping[selector.name][selector.element]
            else:
                slice_value = mapping[selector.name][selector.initial : selector.final]

            mapping[identifier] = slice_value

        return super().safe_substitute(mapping)

    def get_identifiers(self) -> list[str]:
        """Return a list of the valid identifiers in the template."""
        retval = set(super().get_identifiers()) - set(self.slice_selectors.keys())

        for selector in self.slice_selectors.values():
            retval.add(selector.name)

        return list(retval)
