import sys
from ..exceptions import err_exit

# Generic error messages
malformed_filter = 'Found following invalid filters: {}'
maxitem_message = 'Too many items given in field {}, maximum is {}'
# An integer equel to 2 if script is run with python2, and 3 if run with python3
python_version = sys.version_info.major

def is_list_of_strings(object):
    if not isinstance(object, list):
        return False
    for item in object:
        # Note that in python 2.7 these strings are read in as unicode
        if python_version == 2:
            if not (isinstance(item, str) or isinstance(item, unicode)):
                return False
        else:
            if not isinstance(item, str):
                return False
    return True

def validate_somatic_filter(filter, filter_type):
    keys = filter.keys()
    if filter_type == 'variant':
        required_filter_count = 0
        expected_keys = {'annotation', 'allele', 'sample', 'location'}
        if not set(keys).issubset(expected_keys):
            err_exit(malformed_filter.format(str(set(keys).difference(list(expected_keys)))))

        if 'annotation' in keys:
            annotation_filter = filter['annotation']
            sub_keys = annotation_filter.keys()
            expected_sub_keys = {'symbol', 'gene', 'feature', 'hgvsc', 'hgvsp'}
            if not set(sub_keys).issubset(expected_sub_keys):
                err_exit(malformed_filter.format(str(set(sub_keys).difference(list(expected_sub_keys)))))

            if 'symbol' in sub_keys:
                if not is_list_of_strings(annotation_filter['symbol']):
                    err_exit(malformed_filter.format('annotation["symbol"]'))
                if len(annotation_filter['symbol']) > 100:
                    err_exit(maxitem_message.format('annotation["symbol"]',100))
                if annotation_filter['symbol']:
                    required_filter_count += 1

            if 'gene' in sub_keys:
                if not is_list_of_strings(annotation_filter['gene']):
                    err_exit(malformed_filter.format('annotation["gene"]'))
                if len(annotation_filter['gene']) > 100:
                    err_exit(maxitem_message.format('annotation["gene"]',100))
                if annotation_filter['gene']:
                    required_filter_count += 1

            if 'feature' in sub_keys:
                if not is_list_of_strings(annotation_filter['feature']):
                    err_exit(malformed_filter.format('annotation["feature"]'))
                if len(annotation_filter['feature']) > 100:
                    err_exit(maxitem_message.format('annotation["feature"]',100))
                if annotation_filter['feature']:
                    required_filter_count += 1

            if 'hgvsc' in sub_keys:
                if not is_list_of_strings(annotation_filter['hgvsc']):
                    err_exit(malformed_filter.format('annotation["hgvsc"]'))
                if len(annotation_filter['hgvsc']) > 100:
                    err_exit(maxitem_message.format('annotation["hgvsc"]',100))
            
            if 'hgvsp' in sub_keys:
                if not is_list_of_strings(annotation_filter['hgvsp']):
                    err_exit(malformed_filter.format('annotation["hgvsp"]'))
                if len(annotation_filter['hgvsp']) > 100:
                    err_exit(maxitem_message.format('annotation["hgvsp"]',100))
        
        if 'allele' in keys:
            allele_filter = filter['allele']
            sub_keys = allele_filter.keys()
            expected_sub_keys = {'allele_id', 'variant_type'}
            if not set(sub_keys).issubset(expected_sub_keys):
                err_exit(malformed_filter.format(str(set(sub_keys).difference(list(expected_sub_keys)))))

            if 'allele_id' in sub_keys:
                if not is_list_of_strings(allele_filter['allele_id']):
                    err_exit(malformed_filter.format('allele["allele_id"]'))
                if len(allele_filter['allele_id']) > 100:
                    err_exit(maxitem_message.format('allele["allele_id"]',100))
                if allele_filter['allele_id']:
                    required_filter_count += 1

            if 'variant_type' in sub_keys:
                if not is_list_of_strings(allele_filter['variant_type']):
                    err_exit(malformed_filter.format('allele["variant_type"]'))
                for item in allele_filter['variant_type']:
                    if item not in ['SNP', 'INS', 'DEL', 'DUP', 'INV', 'CNV', 'CNV:TR', 'BND', 'DUP:TANDEM', 'DEL:ME', 'INS:ME', 'MISSING', 'MISSING:DEL', 'UNSPECIFIED', 'REF', 'OTHER']:
                        err_exit(malformed_filter.format('allele["variant_type"]'))
                if len(allele_filter['variant_type']) > 10:
                    err_exit(maxitem_message.format('allele["variant_type"]',10))

        if 'sample' in keys:
            sample_filter = filter['sample']
            sub_keys = sample_filter.keys()
            expected_sub_keys = {'sample_id', 'assay_sample_id'}
            if not set(sub_keys).issubset(expected_sub_keys):
                err_exit(malformed_filter.format(str(set(sub_keys).difference(list(expected_sub_keys)))))

            if 'sample_id' in sub_keys:
                if not is_list_of_strings(sample_filter['sample_id']):
                    err_exit(malformed_filter.format('sample["sample_id"]'))
                if len(sample_filter['sample_id']) > 500:
                    err_exit(maxitem_message.format('sample["sample_id"]',500))
            
            if 'assay_sample_id' in sub_keys:
                if not is_list_of_strings(sample_filter['assay_sample_id']):
                    err_exit(malformed_filter.format('sample["assay_sample_id"]'))
                if len(sample_filter['assay_sample_id']) > 1000:
                    err_exit(maxitem_message.format('sample["assay_sample_id"]',1000))
                   
        if 'location' in keys:
            # Ensure there are not more than 100 locations
            if len(filter['location']) > 100:
                print(maxitem_message.format('location',100))
            for indiv_location in filter['location']:
                indiv_loc_keys = indiv_location.keys()
                # Ensure all keys are there
                if not (
                    ('chromosome' in indiv_loc_keys)
                    and ('starting_position' in indiv_loc_keys)
                    and ('ending_position' in indiv_loc_keys)
                ):
                    print(malformed_filter.format('location'))
                    err_exit()
                # Check that each key is a string
                if not is_list_of_strings(list(indiv_location.values())):
                    print(malformed_filter.format('location'))
                    err_exit()
            if filter['location']:
                required_filter_count += 1

    if required_filter_count != 1:
        err_exit('Exactly one of "symbol", "gene", feature", "allele_id" or "location" must be provided in the json')
