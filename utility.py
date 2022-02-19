import json


def get_unique_records(data):
    unique_records = dict()
    output = []

    for record in data:
        id = record["id"]
        unique_records.setdefault(id, 0)
        unique_records[id] += 1

    # print(unique_records)

    for record in data:
        id = record["id"]

        if unique_records[id] == 1:
            output.append(record)

    # print("unique", output)

    return output


def get_valid_column_count():
    valid_column_count = dict()
    with open("unique_records.json") as f:
        data = json.load(f)

        for record in data:
            id_column_name = "id"
            name_column_name = "name"
            age_column_name = "age"

            valid_column_count.setdefault(id_column_name, 0)
            valid_column_count.setdefault(name_column_name, 0)
            valid_column_count.setdefault(age_column_name, 0)

            id_value = record[id_column_name]
            name_value = record[name_column_name]
            age_value = record[age_column_name]

            print(id_value, name_value, age_value)

            if id_value is not None:
                valid_column_count[id_column_name] += 1

            if name_value is not None:
                valid_column_count[name_column_name] += 1

            if age_value is not None:
                valid_column_count[age_column_name] += 1

    return valid_column_count
