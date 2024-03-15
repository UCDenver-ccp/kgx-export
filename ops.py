import json
operations_dict = {}
with open('edges.tsv','r') as infile:
    for line in infile:
        columns = line.split('\t')
        if len(columns) < 13:
            print('not enough columns')
            print(line)
            continue
        subject_namespace = columns[0].split(':')[0]
        object_namespace = columns[2].split(':')[0]
        predicate = 'no_predicate'
        if columns[1] == 'biolink:treats':
            predicate = 'treats'
        elif columns[1] == 'biolink:contributes_to':
            predicate = 'contributes_to'
        elif columns[1] == 'biolink:affects':
            if columns[3] == 'biolink:causes':
                if columns[8] == 'activity_or_abundance' and columns[9] == 'increased':
                    predicate = 'positively_regulates'
                elif columns[8] == 'activity_or_abundance' and columns[9] == 'decreased':
                    predicate = 'negatively_regulates'
            elif columns[3] == 'biolink:contributes_to':
                if columns[7] == 'gain_of_function_variant_form':
                    predicate = 'gain_of_function_contributes_to'
                elif columns[7] == 'loss_of_function_variant_form':
                    predicate = 'loss_of_function_contributes_to'
        key = subject_namespace + '_' + predicate + '_' + object_namespace
        if key in operations_dict:
            operations_dict[key] += 1
        else:
            operations_dict[key] = 1
with open('operations.json','w') as outfile:
    x = outfile.write(json.dumps(operations_dict))