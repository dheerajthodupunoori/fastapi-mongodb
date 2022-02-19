from typing import Optional
import json
from fastapi import FastAPI, Body, File, UploadFile
from pydantic import BaseModel, Field
from fastapi.encoders import jsonable_encoder
from database import (
    add_employee,
    retrieve_employees,
    retrieve_employee,
    delete_employee,
    greet_employee,
    update_employee,
    upload_unique_records_json_file
)
from utility import get_unique_records, get_valid_column_count


class EmployeeSchema(BaseModel):
    id: str = Field(...)
    name: str = Field(...)
    age: int = Field(...)
    doj: str = Field(...)
    dob: str = Field(...)
    aadhaar: str = Field(...)


class UpdateEmployeeModel(BaseModel):
    id: Optional[str]
    name: Optional[str]
    age: Optional[str]
    doj: Optional[str]
    dob: Optional[int]
    aadhaar: Optional[float]


def ResponseModel(data, message):
    return {
        "data": [data],
        "code": 200,
        "message": message,
    }


def ErrorResponseModel(error, code, message):
    return {"error": error, "code": code, "message": message}


app = FastAPI(
    title="Employee information",
    version="1.0.0",
    docs_url="/",
    redoc_url="/docs"
)


@app.post("/addEmployee", response_description="Employee data added into the database")
async def add_employee_data(employee: EmployeeSchema = Body(...)):
    employee = jsonable_encoder(employee)
    new_employee = await add_employee(employee)
    return ResponseModel(new_employee, "Employee added successfully.")


@app.get("/getAllEmployees", response_description="Employees retrieved")
async def get_employees():
    employees = await retrieve_employees()
    if employees:
        return ResponseModel(employees, "Employees data retrieved successfully")
    return ResponseModel(employees, "Empty list returned")


@app.get("/getEmployee/{id}", response_description="Employee data retrieved")
async def get_employee_data(id):
    employee = await retrieve_employee(id)
    if employee:
        return ResponseModel(employee, "Employee data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "Employee doesn't exist.")


@app.delete("/deleteEmployee/{id}", response_description="Employee data deleted from the database")
async def delete_employee_data(id: str):
    deleted_employee = await delete_employee(id)
    if deleted_employee:
        return ResponseModel(
            "Employee with ID: {} removed".format(id), "Employee deleted successfully"
        )
    return ErrorResponseModel(
        "An error occurred", 404, "Employee with id {0} doesn't exist".format(id)
    )


@app.get("/greetEmployees")
async def greet_employees():
    valid_employees_for_greeting = await greet_employee("WORK_ANNIVERSARY")
    if valid_employees_for_greeting:
        return ResponseModel(valid_employees_for_greeting, "Congratulations on your work anniversary")
    return ResponseModel(valid_employees_for_greeting, "No one completed year by today")


@app.get("/wishEmployees")
async def wish_employees():
    valid_employees_for_wishes = await greet_employee("BIRTHDAY")
    if valid_employees_for_wishes:
        return ResponseModel(valid_employees_for_wishes, "Many more happy returns of the day")
    return ResponseModel(valid_employees_for_wishes, "No Birthdays for today")


@app.put("/updateEmployee/{id}")
async def update_employee_data(id: str, req: UpdateEmployeeModel = Body(...)):
    req = {k: v for k, v in req.dict().items() if v is not None}
    updated_employee = await update_employee(id, req)
    if updated_employee:
        return ResponseModel(
            "Employee with ID: {} name update is successful".format(id),
            "Employee name updated successfully",
        )
    return ErrorResponseModel(
        "An error occurred",
        404,
        "There was an error updating the Employee data.",
    )


@app.post("/uploadUniqueRecords")
async def upload_unique_records(file: UploadFile = File(...)):

    try:
        content = await file.read()
        data = json.loads(content)
        unique_records = get_unique_records(data)
        with open("unique_records.json", "w") as output:
            json.dump(unique_records, output, indent=4)

        await upload_unique_records_json_file()
        valid_column_count = get_valid_column_count()
        return True, valid_column_count
    except Exception as e:
        print(e)
        return False
