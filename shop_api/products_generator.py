import json
import random
import uuid
import logging
from datetime import datetime

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_random_product(block_chance=0.3):
    """
    Generates a random product.
    """
    product_id = str(uuid.uuid4())
    name = f"Product {product_id}"

    # 20% chance to generate a product that will be blocked
    if random.random() < block_chance:
        name = "Смартфон телефон Apple Pro Max" # this will trigger the filter
        logger.info(f"Generated product {product_id} with name '{name}' that will be blocked.")
    else:
        logger.info(f"Generated product {product_id} with name '{name}'.")

    description = f"Description of {name}"
    price = {"amount": random.uniform(10, 1000), "currency": random.choice(["USD", "EUR", "RUB"])}
    category = random.choice(["Electronics", "Clothing", "Books"])
    brand = random.choice(["BrandA", "BrandB", "BrandC", "Apple"])
    stock = {"available": random.randint(0, 100), "reserved": random.randint(0, 20)}
    sku = f"SKU-{random.randint(1000, 9999)}"
    tags = random.sample(["tag1", "tag2", "tag3", "tag4", "tag5"], random.randint(1, 3))
    images = [{"url": f"https://example.com/image{i}.jpg", "alt": f"Image {i}"} for i in range(random.randint(1, 3))]
    specifications = {"weight": f"{random.uniform(100, 500)}g", "dimensions": f"{random.uniform(10, 50)}cm"}
    created_at = datetime.now().isoformat()
    updated_at = datetime.now().isoformat()
    index = "products"
    store_id = f"store_{random.randint(1, 5)}"

    return {
        "product_id": product_id,
        "name": name,
        "description": description,
        "price": price,
        "category": category,
        "brand": brand,
        "stock": stock,
        "sku": sku,
        "tags": tags,
        "images": images,
        "specifications": specifications,
        "created_at": created_at,
        "updated_at": updated_at,
        "index": index,
        "store_id": store_id,
    }

def generate_products(num_products, block_chance=0.1):
    """
    Generates a list of random products.
    """
    return [generate_random_product(block_chance) for _ in range(num_products)]

if __name__ == "__main__":
    products = generate_products(5)
    for product in products:
        print(json.dumps(product, indent=2))