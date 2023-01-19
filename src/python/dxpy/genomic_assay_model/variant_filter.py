import json


class Allele:
    def __init__(self, allele_dict={}):
        self.id = allele_dict.get("id") or None
        self.rsid = allele_dict.get("rsid") or None
        self.type = allele_dict.get("type") or None
        self.ref = allele_dict.get("ref") or None
        self.dataset_alt_af = allele_dict.get("dataset_alt_af") or dict()
        self.gnomad_alt_af = allele_dict.get("dataset_alt_af") or dict()

    def generate_filter(self, optimized=False):
        # Select optimized or regular allele table
        allele_filter = {}
        if optimized:
            table = "allele_optimized"
        else:
            table = "allele"

        # This bit looks like a serious violation of DRY principle, but

        # Generate filter for id
        if self.id and self.id != "*":
            filter_key = "{}${}".format(table, "a_id")
            allele_filter[filter_key] = {"condition": "in", "values": self.id}
        # Generate filter for rsid
        if self.rsid and self.rsid != "*":
            filter_key = "{}${}".format(table, "dbsnp151_rsid")
            allele_filter[filter_key] = {"condition": "in", "values": self.rsid}
        # Generate filter for type, which is a list
        if self.type and self.type != "*":
            filter_key = "{}${}".format(table, "allele_type")
            # TODO Might need to use a condition other than "is" here
            allele_filter[filter_key] = {"condition": "in", "values": self.type}
        if self.ref and self.ref != "*":
            # TODO ref might have been removed from the spec
            filter_key = "{}${}".format(table, "ref")
            allele_filter[filter_key] = {"condition": "in", "values": self.ref}
        if self.dataset_alt_af and self.dataset_alt_af != "*":
            # TODO: Can this and the following filter have just min or max, or will they always have both?
            filter_key = "{}${}".format(table, "alt_freq")
            minimum = self.dataset_alt_af["min"]
            maximum = self.dataset_alt_af["max"]
            allele_filter[filter_key] = {
                "condition": "between",
                "values": [minimum, maximum],
            }
        if self.gnomad_alt_af and self.gnomad_alt_af != "*":
            filter_key = "{}${}".format(table, "alt_freq")
            minimum = self.gnomad_alt_af["min"]
            maximum = self.gnomad_alt_af["max"]
            allele_filter[filter_key] = {
                "condition": "between",
                "values": [minimum, maximum],
            }

        # Handle the case where everything is *
        if allele_filter == {}:
            return None

        # Set the logic for all these filters to be "and"
        allele_filter["logic"] = "and"
        allele_filter["name"] = "allele"

        return allele_filter


class Genotype:
    def __init__(self, genotype_dict={}):
        # TODO: see if we can name this differently

        # A list of zygosities represented as strings
        self.genotype = genotype_dict.get("genotype") or None

    def generate_filter(self, optimized=False):
        genotype_filter = {}

        # Select the optimized or normal table
        if optimized:
            table = "genotype_optimized"
        else:
            table = "genotype"

        if self.genotype and self.genotype != "*":
            filter_key = "{}${}".format(table, "type")
            genotype_filter[filter_key] = {"condition": "in", "values": self.genotype}

        # Handle the case where everything is *
        if genotype_filter == {}:
            return None

        genotype_filter["logic"] = "and"
        genotype_filter["name"] = "genotype"

        return genotype_filter


class Location:
    def __init__(self, location_dict={}):
        # Note that the location object is a single element in the list given on the "location" line

        # Handle the case where a single "*" is given rather than a list of location JSON objects
        if isinstance(location_dict, str):
            self.chromosome = "*"
            return None
        else:
            self.chromosome = location_dict.get("chromosome") or None
            self.starting_position = location_dict.get("starting_position") or None
            self.ending_position = location_dict.get("ending_position") or None

    def generate_filter(self, optimized=False):
        location_filter = {}

        # Select the optimized or normal table
        # The location is of the allele, rather than the genotype or annotation table
        if optimized:
            table = "allele_optimized"
        else:
            table = "allele"

        if self.chromosome and self.chromosome != "*":
            filter_key = "{}${}".format(table, "chr")
            location_filter[filter_key] = {"condition": "is", "values": self.chromosome}
        if self.starting_position and self.starting_position != "*":
            filter_key = "{}${}".format(table, "pos")
            location_filter[filter_key] = {
                "condition": "greater-than",
                "values": int(self.starting_position),
            }
        if self.starting_position and self.starting_position != "*":
            filter_key = "{}${}".format(table, "pos")
            location_filter[filter_key] = {
                "condition": "less-than",
                "values": int(self.ending_position),
            }
        # Handle the case where everything is *
        if location_filter == {}:
            return None

        # The filters within a single location are related by and, but the relationship between location filters is or
        location_filter["logic"] = "and"

        return location_filter


class Annotation:
    def __init__(self, annotation_dict={}):
        self.gene_name = annotation_dict.get("gene_name") or None
        self.gene_id = annotation_dict.get("gene_id") or None
        self.feature_id = annotation_dict.get("feature_id") or None
        self.consequences = annotation_dict.get("consequences") or None
        self.putative_impact = annotation_dict.get("putative_impact") or None
        self.hgvs_c = annotation_dict.get("hgvs_c") or None
        self.hgvs_p = annotation_dict.get("hgvs_p") or None

    def generate_filter(self, optimized=False):
        annotation_filter = {}

        # Select the normal or optimized
        if optimized:
            table = "allele_optimized"
        else:
            table = "allele"

        if self.gene_name and self.gene_name != "*":
            filter_key = "{}${}".format(table, "gene_name")
            annotation_filter[filter_key] = {
                "condition": "in",
                "values": self.gene_name,
            }

        if self.gene_id and self.gene_id != "*":
            filter_key = "{}${}".format(table, "gene_id")
            annotation_filter[filter_key] = {
                "condition": "in",
                "values": self.gene_id,
            }
        if self.feature_id and self.feature_id != "*":
            filter_key = "{}${}".format(table, "feature_id")
            annotation_filter[filter_key] = {
                "condition": "in",
                "values": self.feature_id,
            }

        if self.consequences and self.consequences != "*":
            filter_key = "{}${}".format(table, "effect")
            annotation_filter[filter_key] = {
                "condition": "in",
                "values": self.consequences,
            }
        if self.putative_impact and self.putative_impact != "*":
            filter_key = "{}${}".format(table, "putative_impact")
            annotation_filter[filter_key] = {
                "condition": "in",
                "values": self.putative_impact,
            }
        if self.hgvs_c and self.hgvs_c != "*":
            filter_key = "{}${}".format(table, "hgvs_c")
            annotation_filter[filter_key] = {
                "condition": "in",
                "values": self.hgvs_c,
            }
        if self.hgvs_p and self.hgvs_p != "*":
            filter_key = "{}${}".format(table, "hgvs_p")
            annotation_filter[filter_key] = {
                "condition": "in",
                "values": self.hgvs_p,
            }
        if annotation_filter == {}:
            return None
        return annotation_filter


class VariantFilter:
    def __init__(self, full_input_json_path, name, id):
        # Initialize
        # json automatically converts null in JSON to None
        self.name = name
        self.id = id

        with open(full_input_json_path, "r") as infile:
            full_input_json = json.load(infile)

        if "allele" in full_input_json:
            self.allele = Allele(full_input_json["allele"])
        else:
            self.allele = Allele()

        if "genotype" in full_input_json:
            self.genotype = Genotype(full_input_json["genotype"])
        else:
            self.genotype = Genotype()

        # I don't love this because it could lead to self.json being either a list of location objects or a string
        if "location" in full_input_json:
            if isinstance(full_input_json["location"], list):
                self.location = [Location(x) for x in full_input_json["location"]]
            elif isinstance(full_input_json["location"], str):
                self.location = full_input_json["location"]
        else:
            self.location = Location()

        if "annotation" in full_input_json:
            self.annotation = Annotation(full_input_json["annotation"])
        else:
            self.annotation = Annotation()

    def compile_filters(self):
        allele_filter = self.allele.generate_filter()
        genotype_filter = self.genotype.generate_filter()
        location_filter_list = [x.generate_filter() for x in self.location]
        annotation_filter = self.annotation.generate_filter()

        compiled_filter = {
            "filters": {"assay_filter": {"id": self.id, "name": self.name}}
        }

        location_filter = {"compound": [{"filter": x} for x in location_filter_list]}

        if len(location_filter_list) > 1:
            location_filter["compound"].append({"logic": "or"})

        all_filters = []
        if allele_filter:
            all_filters.append(allele_filter)
        if genotype_filter:
            all_filters.append(genotype_filter)
        if location_filter:
            all_filters.append(location_filter)
        if annotation_filter:
            all_filters.append(annotation_filter)

        compiled_filter["filters"]["assay_filter"]["compound"] = [
            {"filters": x} for x in all_filters
        ]

        if len(all_filters) > 0:
            compiled_filter["logic"] = "and"
            return compiled_filter
        else:
            return {}


if __name__ == "__main__":
    # Temporarily hardcode some variables that we will eventually get from the command line
    json_path = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/dxpy/genomic_assay_model/test_input/example_input.json"
    output_file = "test_output/test_output.json"
    name = "testname"
    id = "testid"

    parsed_filter = VariantFilter(json_path, name, id)
    vizserver_json = parsed_filter.compile_filters()

    # Write to a file for examination
    with open(output_file, "w") as outfile:
        json.dump(vizserver_json, outfile)
