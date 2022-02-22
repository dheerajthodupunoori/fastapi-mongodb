import asyncio
import urllib.parse
import motor.motor_asyncio
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


async def upload_unique_records_json_file():
    json_collection = connect_to_mongo_db()
    with open("unique_records.json") as f:
        data = json.load(f)
        await json_collection.insert_many(data)
        # print("length of data after removing duplicates - ", len(data))
        # for count, doc in enumerate(data):
        #     print(count)
        #     await json_collection.insert_one(doc)


def generate_unique_records(data):
    unique_records = []
    unique_set = dict()

    for record in data:
        unique_set.setdefault(record["_id"], 0)
        unique_set[record["_id"]] += 1

    for record in data:
        if unique_set[record["_id"]] == 1:
            unique_records.append(record)

    with open("unique_records.json", "w") as output:
        json.dump(unique_records, output, indent=4)


def valid_column_count():
    unique_data = None
    with open("unique_records.json") as input:
        unique_data = json.load(input)
        print(f"Length of unique data {len(unique_data)}")

    final = dict()

    for record in unique_data:
        # print(record)
        final.setdefault("phone_number", list())
        final.setdefault("website", list())
        final.setdefault("years-with-yp", list())
        final.setdefault("crawled_at", list())
        final.setdefault("name", list())
        final.setdefault("year-in-business", list())
        final.setdefault("tags", list())
        final.setdefault("url", list())
        final.setdefault("track-map", list())
        final.setdefault("location", list())
        final.setdefault("address", list())
        final.setdefault("category", list())
        final.setdefault("thumbnail", list())

        if record["phone_number"] and record["phone_number"] not in final["phone_number"]:
            final["phone_number"].append(record["phone_number"])

        if record["website"] and record["website"] not in final["website"]:
            final["website"].append(record["website"])

        if record["years-with-yp"] and record["years-with-yp"] not in final["years-with-yp"]:
            final["years-with-yp"].append(record["years-with-yp"])

        if record["crawled_at"] and record["crawled_at"] not in final["crawled_at"]:
            final["crawled_at"].append(record["crawled_at"])

        if record["name"] and record["name"] not in final["name"]:
            final["name"].append(record["name"])

        if record["year-in-business"] and record["year-in-business"] not in final["year-in-business"]:
            final["year-in-business"].append(record["year-in-business"])

        if record["tags"] and record["tags"] not in final["tags"]:
            final["tags"].append(record["tags"])

        if record["url"] and record["url"] not in final["url"]:
            final["url"].append(record["url"])

        if record["track-map"] and record["track-map"] not in final["track-map"]:
            final["track-map"].append(record["track-map"])

        if record["location"] and record["location"] not in final["location"]:
            final["location"].append(record["location"])

        if record["address"] and record["address"] not in final["address"]:
            final["address"].append(record["address"])

        if record["category"] and record["category"] not in final["category"]:
            final["category"].append(record["category"])

        if "thumbnail" in record and record["thumbnail"] and record["thumbnail"] not in final["thumbnail"]:
            final["thumbnail"].append(record["thumbnail"])

    final_count = dict()

    for key in final:
        final_count.setdefault(key, len(final[key]))
    print(final_count)


def main():
    try:
        with open("json-data.json") as input:
            data = json.load(input)
        print("Length of data with duplicates - ", len(data))
        generate_unique_records(data)
        return True
    except Exception as e:
        print(e)
        return False


if __name__ == "__main__":
    main()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(upload_unique_records_json_file())
    valid_column_count()
