from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from elasticsearch import Elasticsearch
from kafka import KafkaProducer
from typing import Optional
import json
import uuid

app = FastAPI()

# Elasticsearch setup
es = Elasticsearch(["http://elasticsearch:9200"])

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers=["kafka-0:9092"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Request model for product search with filters
class ProductSearchRequest(BaseModel):
    name: str = "Apple"  # Default value for name
    client_id: uuid.UUID  # Ensures each request has a unique client ID

# Endpoint to search for product by name
@app.post("/search_product/")
async def search_product(request: ProductSearchRequest,
        brand: Optional[str] = None,
        currency: Optional[str] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None
):
    page_size = 100  # Number of results per page
    print(f'{request=}')

    # Build query dynamically based on parameters present in the request
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"name": request.name}}
                ],
                "filter": []
            }
        },
        "size": page_size
    }
    if brand:
        query['query']['bool']['filter'].append({"term": {"brand.keyword": brand}})

    if currency:
        query['query']['bool']['filter'].append({"term": {"currency.keyword": currency}})

    if min_price is not None:
        query['query']['bool']['filter'].append({"range": {"price": {"gte": min_price}}})

    if max_price is not None:
        query['query']['bool']['filter'].append({"range": {"price": {"lte": max_price}}})

    print(f'{query=}')
    all_results = []
    response = es.search(index="filtered_products", body=query, headers={"Content-Type": "application/json", "Accept": "application/json"})

    # Calculate total number of pages
    total_hits = response['hits']['total']['value']
    pages = (total_hits // page_size) + 1

    for page in range(pages):
        query['from'] = page * page_size
        response = es.search(index="filtered_products", body=query, headers={"Content-Type": "application/json", "Accept": "application/json"})
        all_results.extend([hit['_source'] for hit in response['hits']['hits']])

    # If no product is found, raise 404 error
    if not all_results:
        raise HTTPException(status_code=404, detail="Product not found")

    # Send product search event to Kafka for analysis
    producer.send(
        "product_search_topic",
        key=str(request.client_id).encode('utf-8'),
        value={"client_id": str(request.client_id), "product_name": request.name}
    )

    return all_results


# Endpoint to get personalized recommendations
@app.get("/recommendations/")
async def get_recommendations(client_id: uuid.UUID = Query(...)):
    # This is a placeholder for personalized recommendations
    recommendations = [
        {"product_id": 1, "name": "Product 1", "price": 100},
        {"product_id": 2, "name": "Product 2", "price": 200},
    ]

    # Send recommendation event to Kafka for analysis
    producer.send("recommendations_topic", value={"client_id": str(client_id), "recommendations": recommendations})

    return recommendations
