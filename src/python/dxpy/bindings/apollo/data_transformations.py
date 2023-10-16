
def transform_to_expression_matrix(list_of_dicts):
    """
    list_of_dicts: list of dictionaries, each of the form
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
    #       "sample_id":<sample_id>,
    #       <feature_id_1>:<expression_1>,
    #       <feature_id_2>:<expression_2>,
    #        etc.
    #   }
    # 
    # }
    colnames = set()
    for entry in list_of_dicts:
        # Keep track of all seen feature_ids, they will become the columns of our final table
        colnames.add(entry["feature_id"])
        if entry["sample_id"] not in transformed_dict:
            transformed_dict[entry["sample_id"]] = {"sample_id":entry["sample_id"],entry["feature_id"]:entry["expression"]}
        else:
            transformed_dict[entry["sample_id"]][entry["feature_id"]] = entry["expression"]
    colnames = sorted(list(colnames))
    colnames.insert(0,"sample_id")

    # We want the output to be a list of dictionaries, rather than a single dicitonary keyed on sample_id
    dict_list = list(transformed_dict.values())
    for dict_row in dict_list:
        for colname in colnames:
            if colname not in dict_row:
                dict_row[colname] = None

    return (dict_list,colnames)