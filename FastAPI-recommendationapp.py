from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from psycopg2 import connect, sql
import psycopg2
from datetime import datetime, timedelta
import os

# FastAPI initialization
app = FastAPI()

# Define data models with 'CTR' and 'views' columns
class Recommendation(BaseModel):
    advertiser_id: str
    model: str
    product_id: str
    CTR: float | None  # 'CTR' may be NULL for 'Top_views'
    views: int  # 'views' is always an int
    date: str

class RecommendationHistory(BaseModel):
    advertiser_id: str
    #model: str
    product_id: str
    CTR: float | None  # 'CTR' may be NULL
    views: int  # 'views' is always an int
    date: str


# Endpoint para recomendaciones de un modelo particular de un advertiser
@app.get("/recommendations/{adv}/{model}", response_model=list[Recommendation])
def get_recommendations(adv: str, model: str):
    conn = psycopg2.connect(
        database = "postgres",
        user = "user_lmi",
        password = "basededatoslmi",
        host = "db-tp-lmi.cjuseewm8uut.us-east-1.rds.amazonaws.com",
        port = "5432" )
    try:
        cur = conn.cursor()  # Create a cursor to execute SQL queries
        yesterday = (datetime.today().date() - timedelta(days=1)).date()
        
        # Decide which table to query based on the 'model'
        if model.lower() == "top ctr":
            query = sql.SQL(
                "SELECT advertiser_id, 'Top_CTR' AS model, product_id, CTR, views, date "
                "FROM Top_CTR "
                "WHERE advertiser_id = %s AND date = %s"
            )
        elif model.lower() == "top views":
            query = sql.SQL(
                "SELECT advertiser_id, 'Top_views' AS model, product_id, views, date "
                "FROM Top_views "
                "WHERE advertiser_id = %s AND date = %s"
            )
        else:
            raise HTTPException(status_code=400, detail="Invalid model type.")
        
#que pasa si pones un advertiser invalido? No va a devolver nada, columnas vacias.

        cur.execute(query, (adv, yesterday))
        rows = cur.fetchall()
        
        if not rows:
            raise HTTPException(status_code=404, detail="No hay recomendaciones para este advertiser y modelo ayer.")
        
        # Map SQL results to Recommendation model
        return [
            Recommendation(
                advertiser_id=r[0], model=model, product_id=r[2], CTR=r[3], views=r[4], date=str(r[5])
            ) for r in rows
        ]
    
    finally:  # Ensure the cursor and connection are closed
        cur.close()  # Close cursor
        conn.close()  # Close connection


# Endpoint para 7 dias de recomenaciones para un advertiser
@app.get("/history/{adv}/", response_model=list[RecommendationHistory])
def get_recommendation_history(adv: str):
    conn = psycopg2.connect(
        database = "postgres",
        user = "user_lmi",
        password = "basededatoslmi",
        host = "db-tp-lmi.cjuseewm8uut.us-east-1.rds.amazonaws.com",
        port = "5432" )  # Connect to PostgreSQL
    try:
        cur = conn.cursor()  # Create a cursor to execute SQL queries
        last_week = datetime.today().date() - timedelta(days=8)  # 8 dias para atras (una semana para atrás desde ayer)
        
        # Fetch history from both 'Top_CTR' and 'Top_views'
        ctr_query = sql.SQL(
            "SELECT advertiser_id, product_id, CTR, views, date "
            "FROM Top_CTR "
            "WHERE advertiser_id = %s AND date >= %s"
        )
        
        views_query = sql.SQL(
            "SELECT advertiser_id, product_id, views, date "
            "FROM Top_views "
            "WHERE advertiser_id = %s AND date >= %s"
        )

        # Get all history data
        cur.execute(ctr_query, (adv, last_week))
        ctr_rows = cur.fetchall()

        cur.execute(views_query, (adv, last_week))
        views_rows = cur.fetchall()


        if not ctr_rows + views_rows:
            raise HTTPException(status_code=404, detail="No hay recomendaciones para este advertiser en los últimos 7 días.")
                
            # Map SQL results to Recommendation model
            return [
                RecommendationHistory(
                    advertiser_id=r[0], product_id=r[1], CTR=r[2], views=r[3], date=str(r[4])
                ) for r in ctr_rows + views_rows
            ]


        # Combine results and return as RecommendationHistory
    
    finally:  # Ensure the cursor and connection are closed
        cur.close()  # Close cursor
        conn.close()  # Close connection


# Endopoint de stats
@app.get("/stats/")
def get_stats():
    return {"message": "Statistics endpoint. Implementation to be defined."}
