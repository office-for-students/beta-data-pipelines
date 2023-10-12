from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("CourseSearchBuilder/")
async def search_builder():
    return {"message": f"Search Builder"}


@app.get("CourseSearchBuilder/")
async def course_search_builder():
    return {"message": f"Course Search Builder"}


@app.get("CreateDataSet/")
async def create_dataset():
    return {"message": f"create_dataset"}


@app.get("CreateInst/")
async def create_inst():
    return {"message": f"create institution"}


@app.get("EtlPipeline/")
async def etl_pipeline():
    return {"message": f"etl pipeline"}


@app.get("PostcodeSearchBuilder/")
async def postcode_search_builder():
    return {"message": f"post code search builder"}


@app.get("SubjectBuilder/")
async def subject_builder(name: str):
    return {"message": f"subject_builder"}
