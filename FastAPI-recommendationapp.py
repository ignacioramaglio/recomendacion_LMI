from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from psycopg2 import connect, sql
from datetime import datetime, timedelta
import os

# FastAPI initialization
app = FastAPI()

# Connection string for PostgreSQL
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Function to get a database connection
def get_db_connection():
    return connect(DB_CONNECTION_STRING)

# Define data models with 'CTR' and 'views' columns
class Recommendation(BaseModel):
    advertiser_id: str
    model: str
    product_id: str
    CTR: int | None  # 'CTR' may be NULL for 'top views'
    views: int | None  # 'views' may be NULL for 'top CTR'
    date: str

class RecommendationHistory(BaseModel):
    advertiser_id: str
    product_id: str
    CTR: int | None
    views: int | None
    date: str


# Endpoint for recommendations by advertiser and model for today's date
@app.get("/recommendations/{adv}/{model}", response_model=list[Recommendation])
def get_recommendations(adv: str, model: str):
    conn = get_db_connection()  # Connect to PostgreSQL
    try:
        cur = conn.cursor()  # Create a cursor to execute SQL queries
        today = datetime.today().date()  # Get today's date
        
        # SQL query to fetch recommendations based on advertiser and model for today's date
        query = sql.SQL(
            "SELECT advertiser_id, model, product_id, CTR, views, date "
            "FROM recommendations "
            "WHERE advertiser_id = %s AND model = %s AND date = %s"
        )
        cur.execute(query, (adv, model, today))
        rows = cur.fetchall()
        
        if not rows:
            raise HTTPException(status_code=404, detail="No recommendations found for this advertiser and model today.")
        
        # Map SQL results to Recommendation model
        return [
            Recommendation(
                advertiser_id=r[0], model=r[1], product_id=r[2], CTR=r[3], views=r[4], date=str(r[5])
            ) for r in rows
        ]
    finally:  # Ensure the cursor and connection are closed
        cur.close()
        conn.close()


# Placeholder endpoint for statistics
@app.get("/stats/")
def get_stats():
    return {"message": "Statistics endpoint. Implementation to be defined."}


# Endpoint for recommendation history for a given advertiser over the last 7 days
@app.get("/history/{adv}/", response_model=list[RecommendationHistory])
def get_recommendation_history(adv: str):
    conn = get_db_connection()  # Connect to PostgreSQL
    try:
        cur = conn.cursor()  # Create a cursor to execute SQL queries
        last_week = datetime.today().date() - timedelta(days=7)  # Calculate 7 days back
        
        # SQL query to fetch history for the last 7 days
        query = sql.SQL(
            "SELECT advertiser_id, product_id, CTR, views, date "
            "FROM recommendations "
            "WHERE advertiser_id = %s AND date >= %s"
        )
        cur.execute(query, (adv, last_week))
        rows = cur.fetchall()
        
        # Map SQL results to RecommendationHistory model
        return [
            RecommendationHistory(
                advertiser_id=r[0], product_id=r[1], CTR=r[2], views=r[3], date=str(r[4])
            ) for r in rows
        ]
    finally:
        cur.close()  # Close cursor
        conn.close()  # Close connection
