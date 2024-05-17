from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from psycopg2 import connect, sql
import psycopg2
from datetime import datetime, timedelta
import os
from typing import Optional


app = FastAPI()

# Definimos los data models con CTR y Views
class Recommendation(BaseModel):
    advertiser_id: str
    model: str
    product_id: str
    CTR: Optional[float] = None  
    views: Optional[int] = None 
    date: str



class RecommendationHistory(BaseModel):
    advertiser_id: str
    product_id: str
    CTR: Optional[float] = None  
    views: Optional[int] = None  
    date: str

@app.get("/recommendations/{adv}/{model}", response_model=list[Recommendation])
def get_recommendations(adv: str, model: str):
   # Conexión con la base de datos
    conn = psycopg2.connect(
        database = "postgres",
        user = "user_lmi",
        password = "basededatoslmi",
        host = "db-tp-lmi.cjuseewm8uut.us-east-1.rds.amazonaws.com",
        port = "5432" )

    try:
        cur = conn.cursor()  
        yesterday = datetime.now().date() - timedelta(days=1)


        # Query cuando el modelo es top ctr
        if model.lower() == "topctr":
            query = sql.SQL(
                "SELECT advertiser_id, 'Top_CTR' AS model, product_id, CTR, date "
                "FROM Top_CTR "
                "WHERE advertiser_id = %s AND date::date = %s::date"
            )


            cur.execute(query, (adv, yesterday))
            rows = cur.fetchall()
            # En caso que se ingrese un advertiser que no tenga data
            if not rows:
                raise HTTPException(status_code=404, detail="No Top_CTR recommendations found for this advertiser and date.")
            # Mapeo de los resultados SQL al modelo
            return [
                Recommendation(
                    advertiser_id=r[0], model=model, product_id=r[2], CTR=r[3], views=None, date=str(r[4])
                ) for r in rows
            ]
         # Query cuando el modelo es top top views
        elif model.lower() == "topviews":
            query = sql.SQL(
                "SELECT advertiser_id, 'Top_views' AS model, product_id, views, date "
                "FROM Top_views "
                "WHERE advertiser_id = %s AND date::date = %s::date"
            )
            cur.execute(query, (adv, yesterday))
            rows = cur.fetchall()
            if not rows:
                raise HTTPException(status_code=404, detail="No Top_views recommendations found for this advertiser and date.")
            
            return [
                Recommendation(
                    advertiser_id=r[0], model=model, product_id=r[2], CTR=None, views=r[3], date=str(r[4])
                ) for r in rows
            ]
        else:
            raise HTTPException(status_code=400, detail="Invalid model type.")

    finally:  
            cur.close()  
            conn.close()  

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
        cur = conn.cursor()  s
        last_week = datetime.now().date() - timedelta(days=8)  # 8 dias para atras (una semana para atrás desde ayer)
        
        # Buscamos la historia para 'Top_CTR' y 'Top_views'
        ctr_query = sql.SQL(
            "SELECT advertiser_id, product_id, CTR, date "
            "FROM Top_CTR "
            "WHERE advertiser_id = %s AND date::date >= %s::date"
        )
        
        views_query = sql.SQL(
            "SELECT advertiser_id, product_id, views, date "
            "FROM Top_views "
            "WHERE advertiser_id = %s AND date::date >= %s::date"
        )

        cur.execute(ctr_query, (adv, last_week))
        ctr_rows = cur.fetchall()

        cur.execute(views_query, (adv, last_week))
        views_rows = cur.fetchall()


        if not ctr_rows + views_rows:
            raise HTTPException(status_code=404, detail="No hay recomendaciones para este advertiser en los últimos 7 días.")
            return []    
            
        return [
            RecommendationHistory(
                advertiser_id=r[0], product_id=r[1], CTR=r[2], views=None, date=str(r[3])
            ) for r in ctr_rows
        ] + [
            RecommendationHistory(
                advertiser_id=r[0], product_id=r[1], CTR=None, views=r[2], date=str(r[3])
            ) for r in views_rows
            ]

    
    finally:  
        cur.close()  
        conn.close()  


@app.get("/stats/")
def get_stats():
    conn = psycopg2.connect(
        database = "postgres",
        user = "user_lmi",
        password = "basededatoslmi",
        host = "db-tp-lmi.cjuseewm8uut.us-east-1.rds.amazonaws.com",
        port = "5432" )
    try:
        cur = conn.cursor()

        # El advertiser con el producto con mayor views
        cur.execute("SELECT product_id, MAX(views) FROM Top_views GROUP BY product_id ORDER BY MAX(views) DESC LIMIT 1")
        most_viewed_product = cur.fetchone()

        #  El producto con el CTR promedio más alto
        cur.execute("SELECT product_id, AVG(CTR) FROM Top_CTR GROUP BY product_id ORDER BY AVG(CTR) DESC LIMIT 1")
        highest_avg_ctr_product = cur.fetchone()

        # El día con la mayor cantidad de views totales
        cur.execute("SELECT date, SUM(views) FROM Top_views GROUP BY date ORDER BY SUM(views) DESC LIMIT 1")
        highest_views_day = cur.fetchone()

        # CTR promedio por día
        cur.execute("SELECT date, AVG(CTR) FROM Top_CTR GROUP BY date ORDER BY date DESC")
        avg_ctr_day = cur.fetchall()


        return {
            "most_viewed_product": {most_viewed_product[0]: most_viewed_product[1]} if most_viewed_product else None,
            "highest_avg_ctr_product": {highest_avg_ctr_product[0]: float(highest_avg_ctr_product[1])} if highest_avg_ctr_product else None,
            "highest_views_day": {str(highest_views_day[0]): highest_views_day[1]} if highest_views_day else None,
            "avg_ctr_day": {str(date): avg_ctr for date, avg_ctr in avg_ctr_day},
        }

    finally:
        cur.close()
        conn.close()
