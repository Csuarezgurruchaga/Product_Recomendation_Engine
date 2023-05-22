from fastapi import FastAPI
import psycopg2
from . import APIUtils

app = FastAPI()

engine = psycopg2.connect(
    database="postgres",
    host="db-airflow.c6z3l3m7uu0r.us-east-2.rds.amazonaws.com",
    user="postgres_admin",
    password="udesa856",
    port=5432,
)


@app.get("/")
def read_root():
    return {"Working": "API"}


@app.get("/recommendations/{ADV}/{Modelo}")
def recommendations(ADV: str, Modelo: str):
    return APIUtils.query_latest_recommendation(ADV, Modelo, engine)


@app.get("/stats/")
def stats():
    return APIUtils.stats_factory(engine)


@app.get("/history/{ADV}/{Modelo}")
def history(ADV: str, Modelo: str):
    return APIUtils.query_historic_recommendation(ADV, Modelo, engine)
