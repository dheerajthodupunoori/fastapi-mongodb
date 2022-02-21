import json
import motor.motor_asyncio
import urllib.parse
from datetime import datetime
from bson import ObjectId

username = urllib.parse.quote_plus('poc_fastapi')
password = urllib.parse.quote_plus("poc@fastapi")

MONGO_DETAILS = "mongodb+srv://{}:{}@cluster0.o7fdi.mongodb.net/employeedb?retryWrites=true&w=majority".format(
    username, password)

client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)

database = client.employeedb

employee_collection = database.get_collection("employee")
json_collection = database.get_collection("json_collection")


def employee_helper(employee) -> dict:
    return {
        "id": str(employee["id"]),
        "name": employee["name"],
        "age": employee["age"],
        "doj": employee["doj"],
        "dob": employee["dob"],
        "aadhaar": employee["aadhaar"],
    }


async def add_employee(employee_data: dict) -> dict:
    employee = await employee_collection.insert_one(employee_data)
    new_employee = await employee_collection.find_one({"_id": employee.inserted_id})
    return employee_helper(new_employee)


async def retrieve_employees():
    employees = []
    async for employee in employee_collection.find():
        employees.append(employee_helper(employee))
    return employees


async def retrieve_employee(id: str) -> dict:
    employee = await employee_collection.find_one({"_id": ObjectId(id)})
    if employee:
        return employee_helper(employee)


async def delete_employee(id: str):
    employee = await employee_collection.find_one({"_id": ObjectId(id)})
    if employee:
        await employee_collection.delete_one({"_id": ObjectId(id)})
        return True


async def greet_employee(greeting_type: str):
    employees = []
    async for employee in employee_collection.find():
        employee_data = employee_helper(employee)
        if greeting_type == "WORK_ANNIVERSARY":
            date_of_joining = employee_data["doj"]
            date_of_joining = datetime.strptime(date_of_joining, '%m/%d/%Y')
            joined_month = date_of_joining.month
            joined_day = date_of_joining.day

            current_date = datetime.today()
            current_month = current_date.month
            current_day = current_date.day

            if joined_month == current_month and joined_day == current_day:
                employees.append(employee_data)
        else:
            date_of_birth = employee_data["dob"]
            date_of_birth = datetime.strptime(date_of_birth, '%m/%d/%Y')
            birth_month = date_of_birth.month
            birth_day = date_of_birth.day

            current_date = datetime.today()
            current_month = current_date.month
            current_day = current_date.day

            if birth_month == current_month and birth_day == current_day:
                employees.append(employee_data)
    return employees


async def update_employee(id: str, data: dict):
    # Return false if an empty request body is sent.
    if len(data) < 1:
        return False
    employee = await employee_collection.find_one({"_id": ObjectId(id)})
    if employee:
        updated_employee = await employee_collection.update_one(
            {"_id": ObjectId(id)}, {"$set": data}
        )
        if updated_employee:
            return True
        return False


async def upload_unique_records_json_file():
    with open("unique_records.json") as f:
        data = json.load(f)
        json_collection.insert_many(data)
