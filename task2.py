import urllib.parse
import motor.motor_asyncio
from utility import get_unique_records, get_valid_column_count
import json


def connect_to_mongo_db():
    username = urllib.parse.quote_plus('poc_fastapi')
    password = urllib.parse.quote_plus("poc@fastapi")

    MONGO_DETAILS = "mongodb+srv://{}:{}@cluster0.o7fdi.mongodb.net/employeedb?retryWrites=true&w=majority".format(
        username, password)
    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
    database = client.employeedb
    json_collection = database.get_collection("json_collection")

    return json_collection


def upload_unique_records_json_file():
    json_collection = connect_to_mongo_db()
    with open("unique_records.json") as f:
        data = json.load(f)
        json_collection.insert_many(data)


def main():
    try:

        with open("data.json") as input:
            data = json.load(input)

        unique_records = get_unique_records(data)
        with open("unique_records1.json", "w") as output:
            json.dump(unique_records, output, indent=4)

        upload_unique_records_json_file()
        valid_column_count = get_valid_column_count()
        return True, valid_column_count
    except Exception as e:
        print(e)
        return False


if __name__ == "__main__":
    result = main()
    print(result)
