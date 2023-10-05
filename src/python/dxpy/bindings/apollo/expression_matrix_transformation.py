from copy import deepcopy

def expression_transform(results_list):
    """
    results_list: list of dictionaries, each of the form
    {
        "feature_id":<feature_id>,
        "sample_id":<sample_id>,
        "expression":<expression>
    }
    Additional key:value pairs are possible, but are not included in the transformed output

    How it works:
    First, create a dictionary where each key is a sample id, and the values are the feature_id:expression pairs
    Then, convert it to a list of dictionaries (the format that output handling is expecting)
    Each item in the list is a dictionary containing one "sample_id":sample_id pair, and
    multiple feature_id:expression pairs

    Returns:
    A list of dictionaries, each of the form:
    {
        "sample_id":<sample_id>,
        <feature_id_1>:<expression_1>
        <feature_id_2>:<expression_2>
        ...
        <feature_id_n>:<expression_n>
    }
    """
    transformed_dict = {}
    # create a dict of the form
    # {
    #   <sample_id>:{
    #       <feature_id_1>:<expression_1>,
    #       <feature_id_2>:<expression_2>,
    #        etc.
    #   }
    # 
    # }
    for entry in results_list:
        if entry["sample_id"] not in transformed_dict:
            transformed_dict[entry["sample_id"]] = {entry["feature_id"]:entry["expression"]}
        else:
            transformed_dict[entry["sample_id"]][entry["feature_id"]] = entry["expression"]

    # Modify the above dictionary into a list of dictionaries, essentially moving the key of each
    # dict into the dict itself
    dict_list = []
    for sample in transformed_dict:
        samp_row = deepcopy(transformed_dict[sample])
        samp_row["sample_id"] = sample
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