from collections import OrderedDict

def expression_transform(results_list):
    # First, create a dictionary where each key is a sample id, and the values are the feature_id:expression pairs
    # Then, convert it to a list of dictionaries (the format that output handling is expecting)
    # Each item in the list is a dictionary containing one "sample_id":sample_id pair, and
    # multiple feature_id:expression pairs

    transformed_dict = {}
    for entry in results_list:
        if entry["sample_id"] not in transformed_dict:
            transformed_dict[entry["sample_id"]] = {entry["feature_id"]:entry["expression"]}
        else:
            transformed_dict[entry["sample_id"]][entry["feature_id"]] = entry["expression"]

    dict_list = []
    for sample in transformed_dict:
        samp_row = OrderedDict([("sample_id",sample)])
        for (feature_id,expression) in transformed_dict[sample].items():
            samp_row[feature_id] = expression
        dict_list.append(samp_row)
    
    # Get the column names that the output writer will use to generate the table
    # Can't use set here because we want the order of the columns to be deterministic
    colnames = []
    for row in results_list:
        row_colname = row["feature_id"] 
        if not row_colname in colnames:
            colnames.append(row_colname)
    # add sample_id to front
    colnames.insert(0,"sample_id")

    return (dict_list,colnames)