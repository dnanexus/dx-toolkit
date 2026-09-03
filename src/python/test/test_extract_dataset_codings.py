#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.
"""
Offline tests for retrieving dataset codings from the vizserver codings API, as used by
`dx extract_dataset -ddd`. No platform access is required; the API is mocked.
"""
from __future__ import print_function, unicode_literals, division, absolute_import

import argparse
import os
import shutil
import tempfile
import unittest
import unittest.mock
from unittest.mock import patch

import pandas as pd

from dxpy.cli import dataset_utilities
from dxpy.cli.dataset_utilities import (
    CODING_PAGINATED_KEY,
    DXDatasetDictionary,
    get_codings,
)
from dxpy.exceptions import DXError

# dataset_utilities imports pandas lazily, inside extract_dataset(), so the module global
# has to be bound before the dictionary classes can be used directly.
dataset_utilities.pd = pd

VISUALIZE_RESP = {"url": "https://vizserver.example", "dataset": "record-xxxx"}


def column(dframe, name):
    """A dataframe column as a list, with pandas' nulls normalized to None. A null here
    is written as an empty cell by DXDatasetDictionary.write()."""
    return [None if pd.isna(value) else value for value in dframe[name]]


def flat_coding(name, codes_to_meanings, codes_to_concepts=None, display=None):
    """A whole non-hierarchical coding, in the shape both the API and descriptor use."""
    return {
        "name": name,
        "codes_to_meanings": codes_to_meanings,
        "codes_to_concepts": codes_to_concepts or {},
        "display": display if display is not None else list(codes_to_meanings),
    }


def descriptor_stub(fields, codings=None):
    """A minimal stand-in for DXDatasetDescriptor.

    `fields` maps field name to (coding_name, is_hierarchical). `codings` is the
    descriptor's own codings block, omitted entirely when None to mimic a descriptor
    written after codings moved into the database.
    """
    model = {
        "entities": {
            "patient": {
                "name": "patient",
                "fields": {
                    field: {
                        "name": field,
                        "coding_name": coding_name,
                        "is_hierarchical": is_hierarchical,
                    }
                    for field, (coding_name, is_hierarchical) in fields.items()
                },
            }
        },
        "global_primary_key": {"entity": "patient", "field": "eid"},
    }
    if codings is not None:
        model["codings"] = codings

    class _Descriptor(object):
        pass

    descriptor = _Descriptor()
    descriptor.model = model
    descriptor.join_info = []
    return descriptor


class TestGetCodings(unittest.TestCase):
    """Pagination and reassembly in get_codings()."""

    def call(self, pages, **kwargs):
        with patch.object(dataset_utilities.dxpy, "DXHTTPRequest") as request:
            request.side_effect = pages
            codings = get_codings(VISUALIZE_RESP, "project-xxxx", **kwargs)
        return codings, request

    def test_single_page(self):
        codings, request = self.call(
            [
                {
                    "results": {
                        "sex": flat_coding("sex", {"0": "Female", "1": "Male"}),
                    },
                    "next": None,
                }
            ]
        )
        self.assertEqual(list(codings), ["sex"])
        self.assertEqual(codings["sex"]["codes_to_meanings"]["1"], "Male")
        self.assertNotIn(CODING_PAGINATED_KEY, codings["sex"])

        # One request, to the codings route, with the project context and no cursor.
        self.assertEqual(request.call_count, 1)
        _, kwargs = request.call_args
        self.assertEqual(
            kwargs["resource"],
            "https://vizserver.example/viz-meta/3.0/record-xxxx/codings",
        )
        self.assertFalse(kwargs["prepend_srv"])
        self.assertEqual(kwargs["data"], {"project_context": "project-xxxx"})

    def test_empty_result_is_trusted(self):
        codings, request = self.call([{"results": {}, "next": None}])
        self.assertEqual(codings, {})
        self.assertEqual(request.call_count, 1)

    def test_whole_codings_across_pages(self):
        cursor = {"name": "sex", "after_code": None}
        codings, request = self.call(
            [
                {
                    "results": {"ethnicity": flat_coding("ethnicity", {"1": "White"})},
                    "next": cursor,
                },
                {
                    "results": {"sex": flat_coding("sex", {"0": "Female"})},
                    "next": None,
                },
            ]
        )
        self.assertEqual(list(codings), ["ethnicity", "sex"])
        self.assertFalse(
            any(CODING_PAGINATED_KEY in coding for coding in codings.values())
        )

        # The cursor is echoed back verbatim as `starting`.
        self.assertEqual(request.call_count, 2)
        _, kwargs = request.call_args
        self.assertEqual(kwargs["data"]["starting"], cursor)

    def test_split_flat_coding_is_merged(self):
        codings, _ = self.call(
            [
                {
                    "results": {
                        "sex": flat_coding("sex", {"0": "Female"}, display=["0"])
                    },
                    "next": {"name": "sex", "after_code": "0"},
                },
                {
                    "results": {
                        "sex": flat_coding("sex", {"1": "Male"}, display=["1"])
                    },
                    "next": None,
                },
            ]
        )
        self.assertEqual(
            codings["sex"]["codes_to_meanings"], {"0": "Female", "1": "Male"}
        )
        self.assertEqual(codings["sex"]["display"], ["0", "1"])
        self.assertTrue(codings["sex"][CODING_PAGINATED_KEY])

    def test_split_hierarchical_coding_is_merged(self):
        codings, _ = self.call(
            [
                {
                    "results": {
                        "icd10": {
                            "name": "icd10",
                            "codes_to_meanings": {"A": "Chapter A"},
                            "codes_to_concepts": {},
                            "codes_to_parent": {},
                        }
                    },
                    "next": {"name": "icd10", "after_code": "A"},
                },
                {
                    "results": {
                        "icd10": {
                            "name": "icd10",
                            "codes_to_meanings": {"A01": "Typhoid"},
                            "codes_to_concepts": {},
                            "codes_to_parent": {"A01": "A"},
                        }
                    },
                    "next": None,
                },
            ]
        )
        self.assertEqual(codings["icd10"]["codes_to_parent"], {"A01": "A"})
        self.assertEqual(
            codings["icd10"]["codes_to_meanings"], {"A": "Chapter A", "A01": "Typhoid"}
        )
        self.assertTrue(codings["icd10"][CODING_PAGINATED_KEY])

    def test_stored_coding_does_not_alias_response(self):
        page_coding = flat_coding("sex", {"0": "Female"}, display=["0"])
        codings, _ = self.call(
            [
                {"results": {"sex": page_coding}, "next": {"name": "sex", "after_code": "0"}},
                {
                    "results": {
                        "sex": flat_coding("sex", {"1": "Male"}, display=["1"])
                    },
                    "next": None,
                },
            ]
        )
        self.assertEqual(codings["sex"]["display"], ["0", "1"])
        # The first page's coding is untouched by the merge.
        self.assertEqual(page_coding["display"], ["0"])
        self.assertEqual(page_coding["codes_to_meanings"], {"0": "Female"})

    def test_stored_coding_copies_unmerged_fields_too(self):
        # A field this function never reads or merges (e.g. a future addition to the wire
        # shape) must still be copied, not aliased, since it is deep-copied wholesale.
        page_coding = flat_coding("sex", {"0": "Female"})
        page_coding["some_future_field"] = {"nested": ["mutable"]}
        codings, _ = self.call([{"results": {"sex": page_coding}, "next": None}])
        codings["sex"]["some_future_field"]["nested"].append("changed")
        self.assertEqual(page_coding["some_future_field"], {"nested": ["mutable"]})

    def test_non_terminating_pagination_raises(self):
        # A server bug that never returns `next: null` must not hang -ddd forever.
        with patch.object(dataset_utilities, "MAX_CODINGS_PAGES", 3):
            with self.assertRaises(DXError):
                self.call(
                    [
                        {
                            "results": {"sex": flat_coding("sex", {"0": "Female"})},
                            "next": {"name": "sex", "after_code": "0"},
                        }
                    ]
                    * 5
                )

    def test_limit_is_sent_only_when_given(self):
        _, request = self.call([{"results": {}, "next": None}], limit=5)
        _, kwargs = request.call_args
        self.assertEqual(kwargs["data"]["limit"], 5)

    def test_error_in_body_raises(self):
        with self.assertRaises(DXError):
            self.call(
                [{"error": {"type": "InvalidInput", "message": "bad project context"}}]
            )

    def test_transport_error_propagates(self):
        with self.assertRaises(ValueError):
            self.call([ValueError("404 Not Found")])

    def test_failure_on_later_page_propagates(self):
        with self.assertRaises(ValueError):
            self.call(
                [
                    {
                        "results": {"sex": flat_coding("sex", {"0": "Female"})},
                        "next": {"name": "ethnicity", "after_code": None},
                    },
                    ValueError("connection reset"),
                ]
            )


class TestCodingDictionary(unittest.TestCase):
    """Building the coding dictionary from API codings versus descriptor codings."""

    def build(self, descriptor, codings):
        """The coding dictionary alone.

        The data and entity dictionaries need a full field schema that has nothing to do
        with codings, so they are stubbed out; __init__ still runs, which is what passes
        `codings` through to load_coding_dictionary().
        """
        with patch.object(
            DXDatasetDictionary, "load_data_dictionary", return_value={}
        ), patch.object(DXDatasetDictionary, "load_entity_dictionary", return_value={}):
            return DXDatasetDictionary(descriptor, codings=codings)

    def coding_frame(self, dictionary, name):
        return dictionary.coding_dictionary[name].reset_index(drop=True)

    def test_flat_and_hierarchical_codings(self):
        descriptor = descriptor_stub(
            {"sex": ("sex", False), "diagnosis": ("icd10", True)}
        )
        codings = {
            "sex": flat_coding(
                "sex", {"0": "Female", "1": "Male"}, {"0": "snomed:1", "1": ""}
            ),
            "icd10": {
                "name": "icd10",
                "codes_to_meanings": {"A": "Chapter A", "A01": "Typhoid"},
                "codes_to_concepts": {},
                "display": [{"A": ["A01"]}],
            },
        }
        dictionary = self.build(descriptor, codings)

        sex = self.coding_frame(dictionary, "sex")
        self.assertEqual(list(sex["code"]), ["0", "1"])
        self.assertEqual(list(sex["meaning"]), ["Female", "Male"])
        self.assertEqual(column(sex, "concept"), ["snomed:1", ""])
        self.assertEqual(list(sex["display_order"]), [1, 2])
        self.assertEqual(list(sex["coding_name"]), ["sex", "sex"])
        self.assertNotIn("parent_code", sex.columns)

        icd10 = self.coding_frame(dictionary, "icd10")
        self.assertEqual(list(icd10["code"]), ["A", "A01"])
        self.assertEqual(list(icd10["parent_code"]), ["", "A"])
        self.assertEqual(list(icd10["meaning"]), ["Chapter A", "Typhoid"])
        self.assertEqual(list(icd10["display_order"]), [1, 2])

    def test_only_field_referenced_codings_are_kept(self):
        descriptor = descriptor_stub({"sex": ("sex", False)})
        codings = {
            "sex": flat_coding("sex", {"0": "Female"}),
            # Present in dx_codings but referenced by no field.
            "sites": flat_coding("sites", {"S1": "Site 1"}),
        }
        dictionary = self.build(descriptor, codings)
        self.assertEqual(list(dictionary.coding_dictionary), ["sex"])

    def test_referenced_coding_missing_from_source_is_skipped(self):
        descriptor = descriptor_stub(
            {"sex": ("sex", False), "diagnosis": ("icd10", True)}
        )
        codings = {"sex": flat_coding("sex", {"0": "Female"})}
        dictionary = self.build(descriptor, codings)
        self.assertEqual(list(dictionary.coding_dictionary), ["sex"])

    def test_block_order_follows_first_field_reference(self):
        descriptor = descriptor_stub(
            {"sex": ("sex", False), "ethnicity": ("ethnicity", False)}
        )
        # Reverse order from the descriptor's fields, as the API sorts by coding name.
        codings = {
            "ethnicity": flat_coding("ethnicity", {"1": "White"}),
            "sex": flat_coding("sex", {"0": "Female"}),
        }
        dictionary = self.build(descriptor, codings)
        self.assertEqual(list(dictionary.coding_dictionary), ["sex", "ethnicity"])

    def test_split_flat_coding_has_no_display_order(self):
        descriptor = descriptor_stub({"sex": ("sex", False)})
        coding = flat_coding("sex", {"0": "Female", "1": "Male"}, display=["0", "1"])
        coding[CODING_PAGINATED_KEY] = True
        dictionary = self.build(descriptor, {"sex": coding})

        sex = self.coding_frame(dictionary, "sex")
        self.assertNotIn("display_order", sex.columns)
        self.assertEqual(list(sex["code"]), ["0", "1"])

    def test_split_hierarchical_coding_uses_parent_edges(self):
        descriptor = descriptor_stub({"diagnosis": ("icd10", True)})
        coding = {
            "name": "icd10",
            "codes_to_meanings": {"A": "Chapter A", "A01": "Typhoid"},
            "codes_to_concepts": {"A01": "snomed:2"},
            "codes_to_parent": {"A01": "A"},
            CODING_PAGINATED_KEY: True,
        }
        dictionary = self.build(descriptor, {"icd10": coding})

        icd10 = self.coding_frame(dictionary, "icd10")
        self.assertEqual(list(icd10["code"]), ["A", "A01"])
        self.assertEqual(list(icd10["parent_code"]), ["", "A"])
        self.assertEqual(column(icd10, "concept"), [None, "snomed:2"])
        self.assertNotIn("display_order", icd10.columns)

    def test_concepts_out_of_order_stay_row_aligned(self):
        descriptor = descriptor_stub({"sex": ("sex", False)})
        codings = {
            "sex": flat_coding(
                "sex",
                {"0": "Female", "1": "Male"},
                # Ordered differently from codes_to_meanings, and missing a code.
                codes_to_concepts={"1": "snomed:male"},
                display=["0", "1"],
            )
        }
        dictionary = self.build(descriptor, codings)

        sex = self.coding_frame(dictionary, "sex")
        self.assertEqual(list(sex["code"]), ["0", "1"])
        self.assertEqual(list(sex["meaning"]), ["Female", "Male"])
        self.assertEqual(column(sex, "concept"), [None, "snomed:male"])

    def test_empty_api_result_yields_no_codings(self):
        descriptor = descriptor_stub({"sex": ("sex", False)})
        dictionary = self.build(descriptor, {})
        self.assertEqual(dictionary.coding_dictionary, {})

    def test_falls_back_to_descriptor_codings(self):
        descriptor = descriptor_stub(
            {"sex": ("sex", False)},
            codings={"sex": flat_coding("sex", {"0": "Female", "1": "Male"})},
        )
        dictionary = self.build(descriptor, None)

        sex = self.coding_frame(dictionary, "sex")
        self.assertEqual(list(sex["code"]), ["0", "1"])
        self.assertEqual(list(sex["display_order"]), [1, 2])

    def test_descriptor_without_codings_block_does_not_raise(self):
        # A descriptor written after codings moved into dx_codings, with no API result to
        # fall back on: no codings, and no crash.
        descriptor = descriptor_stub({"sex": ("sex", False)})
        dictionary = self.build(descriptor, None)
        self.assertEqual(dictionary.coding_dictionary, {})


class TestDumpDatasetDictionaryCodings(unittest.TestCase):
    """The -ddd path: codings come from the API, or from the descriptor if it is unavailable."""

    def run_ddd(self, pages, descriptor_codings=None):
        """Runs extract_dataset -ddd against a mocked platform, returning the codings CSV
        path (which is absent when no codings were written)."""
        out_directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, out_directory)

        descriptor = descriptor_stub(
            {"sex": ("sex", False)}, codings=descriptor_codings
        )
        # Mirrors DXDatasetDescriptor.get_dictionary().
        descriptor.get_dictionary = lambda codings=None: DXDatasetDictionary(
            descriptor, codings=codings
        )
        dataset = unittest.mock.Mock()
        dataset.get_descriptor.return_value = descriptor

        visualize_resp = dict(VISUALIZE_RESP, recordName="ds", recordTypes=["Dataset"])
        args = argparse.Namespace(
            path="record-xxxx",
            fields=None,
            fields_file=None,
            sql=False,
            dump_dataset_dictionary=True,
            list_fields=False,
            list_entities=False,
            delim=",",
            output=out_directory,
        )

        with patch.object(
            dataset_utilities,
            "resolve_validate_record_path",
            return_value=(
                "project-xxxx",
                {"id": "record-xxxx"},
                visualize_resp,
                "project-yyyy",
            ),
        ), patch.object(
            dataset_utilities, "DXDataset", return_value=dataset
        ), patch.object(
            dataset_utilities.dxpy, "DXHTTPRequest"
        ) as request, patch.object(
            DXDatasetDictionary, "load_data_dictionary", return_value={}
        ), patch.object(
            DXDatasetDictionary, "load_entity_dictionary", return_value={}
        ):
            request.side_effect = pages
            dataset_utilities.extract_dataset(args)

        return os.path.join(out_directory, "ds.codings.csv")

    def test_codings_come_from_the_api(self):
        codings_csv = self.run_ddd(
            [
                {
                    "results": {
                        "sex": flat_coding("sex", {"0": "Female", "1": "Male"})
                    },
                    "next": None,
                }
            ],
            # A descriptor written before codings moved into the database would carry a
            # stale block; the API's codings must win.
            descriptor_codings={"sex": flat_coding("sex", {"9": "Stale"})},
        )
        written = pd.read_csv(codings_csv, dtype=str)
        self.assertEqual(list(written["code"]), ["0", "1"])
        self.assertEqual(list(written["meaning"]), ["Female", "Male"])

    def test_falls_back_to_descriptor_when_api_fails(self):
        codings_csv = self.run_ddd(
            [ValueError("404 Not Found")],
            descriptor_codings={"sex": flat_coding("sex", {"0": "Female"})},
        )
        written = pd.read_csv(codings_csv, dtype=str)
        self.assertEqual(list(written["code"]), ["0"])
        self.assertEqual(list(written["meaning"]), ["Female"])

    def test_no_codings_file_when_dataset_has_none(self):
        codings_csv = self.run_ddd([{"results": {}, "next": None}])
        self.assertFalse(os.path.exists(codings_csv))


if __name__ == "__main__":
    unittest.main()
