# FASTAPI + DASK

Sample code demonstrating usage of DASK from FastAPI.

Steps to run: 

1) Set up a virtual environment for Python 3 and enable it
2) Run command `pip install -r requirements.txt` to install the dependencies
3) Run command `uvicorn main:app --reload` to start web server
4) Enter the request in the url http://127.0.0.1:8000/api/salaries/avg

```json
[
    {
        "id":2598,
        "name":"86.10 - Деятельность больничных организаций",
        "value":43913.26684799033
    },
    {
        "id":2575,
        "name":"85.13 - Образование основное общее",
        "value":38907.57281553398
    },{
        "id":2576,
        "name":"85.14 - Образование среднее общее",
        "value":37156.72058003346}
]
```

5) Enter the request in the url http://127.0.0.1:8000/api/salaries/distribution

```json
[
    {
        "name":"менее 10 т",
        "value":578,
        "percent":4.164865254359418
    },
    {
        "name":"10 000 - 15 000 руб.",
        "value":600,
        "percent":4.323389537397319
    },
    {
        "name":"15 000 - 30 000 руб.",
        "value":5001,
        "percent":36.03545179420666
    },
    {
        "name":"30 000 - 50 000 руб.",
        "value":4782,
        "percent":34.457414613056635
    },
    {
        "name":"50 000 - 80 000 руб.",
        "value":2000,
        "percent":14.411298457991064
    },
    {
        "name":"свыше 80 000",
        "value":917,
        "percent":6.607580342988903
    }
]
```
