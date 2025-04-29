from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from elasticsearch import Elasticsearch
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from typing import Optional
import json
import uuid

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = "kafka-0:9092"
RECOMMENDATIONS_TOPIC = "client_recommendations"
PRODUCT_SEARCH_TOPIC = "product_search_topic"

# Elasticsearch setup
es = Elasticsearch(["http://elasticsearch:9200"])

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Request model for product search with filters
class ProductSearchRequest(BaseModel):
    name: str = "Apple"  # Default value for name
    client_id: uuid.UUID  # Ensures each request has a unique client ID

# Endpoint to search for product by name
@app.post("/search_product/")
async def search_product(
        request: ProductSearchRequest,
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
        PRODUCT_SEARCH_TOPIC,
        key=request.client_id,
        value={
            "client_id": str(request.client_id),
            "product_name": request.name,
            "results": all_results
        }
    )

    return all_results


# Endpoint to get personalized recommendations
@app.get("/recommendations/")
async def get_recommendations(client_id: uuid.UUID = Query(...)):
    client_id_str = str(client_id)

    consumer = KafkaConsumer(
        RECOMMENDATIONS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        group_id='test',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )

    latest_message = None

    try:
        for message in consumer.poll(timeout_ms=5000, max_records=10).values():
            for record in message:
                latest_message = record.key
                if record.key == client_id_str:
                    latest_message = {"recommendation": record.value}

    finally:
        consumer.close()

    if latest_message:
        return latest_message
    else:
        return {"detail": f"No recommendation found for client_id {client_id}"}