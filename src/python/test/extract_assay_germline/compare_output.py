# To be run within a docker container that the dxpy package has been build locally in
# exucute with: python /dx-toolkit/src/python/test/extract_assay_germline/compare_output.py

import os
import pandas as pd

test_output_dir = "/dx-toolkit/src/python/test/extract_assay_germline/test_output"
expected_output_dir = (
    "/dx-toolkit/src/python/test/extract_assay_germline/expected_output"
)

test_output_file_names = os.listdir(test_output_dir)
expected_output_file_names = os.listdir(expected_output_dir)

# Hardcoding in just the first allele test for now
# test_output_file_names = ["allele_rsid_output.tsv"]
# expected_output_file_names = ["allele_rsid_output.tsv"]
# filter_type = "allele"

# Counters of passed and failed tests
num_passed = 0
num_failed = 0

for test_file_name in test_output_file_names:
    if test_file_name[0:6] == "allele":
        filter_type = "allele"
    elif test_file_name[0:10] == "annotation":
        filter_type = "annotation"
    elif test_file_name[0:8] == "genotype":
        filter_type = "genotype"
    # First, check if there is an expected output file.  Expected output files have same naem
    if test_file_name in expected_output_file_names:
        test_output = pd.read_csv(
            os.path.join(test_output_dir, test_file_name), sep="\t", dtype=str
        )
        expected_output = pd.read_csv(
            os.path.join(expected_output_dir, test_file_name), sep="\t", dtype=str
        )

        # Handle the formatting of the rsid field in the extract_assay germline output
        if filter_type == "allele":
            test_output["unlisted_rsid"] = test_output["rsid"].apply(
                lambda x: x[2 : len(x) - 2] if x is not None else None
            )
            test_output["rsid"] = test_output["unlisted_rsid"]
            test_output = test_output.drop("unlisted_rsid", axis=1)
        elif filter_type == "annotation":
            test_output["unlisted_consequences"] = test_output["consequences"].apply(
                lambda x: x[2 : len(x) - 2] if x is not None else None
            )
            test_output["consequences"] = test_output["unlisted_consequences"]
            test_output = test_output.drop("unlisted_consequences", axis=1)

        # TODO remove this when we solve the duplicate problem
        test_output = test_output.drop_duplicates()

        # Sort the tables before comparing
        test_output = test_output.sort_values(by="allele_id")
        expected_output = expected_output.sort_values(by="allele_id")

        if test_output.equals(expected_output):
            print("Test {} output correct".format(test_file_name))
            num_passed += 1
        else:
            print("FAILED test {}, output incorrect".format(test_file_name))
            num_failed += 1
            # Print both dataframes for manual inspection
            with pd.option_context(
                "display.max_rows", None, "display.max_columns", None
            ):
                pass
                # print(test_output)
                # print(expected_output)

        # Checking each column individually
        """  columns = test_output.columns.tolist()
        for column in columns:
            if test_output[column].equals(expected_output[column]):
                print("column {} identical".format(column))
            else:
                print("FAILED column {} not identical".format(column))"""

    else:
        print("No expected output file found for {}".format(test_file_name))
