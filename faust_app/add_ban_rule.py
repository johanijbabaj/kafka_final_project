from kafka import KafkaProducer
import json
import sys

producer = KafkaProducer(bootstrap_servers='kafka-0:9092')

json_path = sys.argv[1]

with open(json_path, 'r') as f:
    rule = json.load(f)

field = rule.get("field")
operator = rule.get("operator")
value = rule.get("value")
currency = rule.get("currency")


# Rules validaion
valid_fields = ["price", "name", "category", "brand", "store_id"]
valid_operators = [">", "<", "=", "contains", "not contains"]

if field not in valid_fields:
    print(f"Invalid field: {field}. Valid fields are: {valid_fields}")
    sys.exit(1)

if operator not in valid_operators:
    print(f"Invalid operator: {operator}. Valid operators are: {valid_operators}")
    sys.exit(1)

if field == "price":
    try:
        float(value)
        if currency is None:
            print("Currency is required for price field.")
            sys.exit(1)
    except ValueError:
        print(f"Invalid value for price: {value}. Must be a number.")
        sys.exit(1)
elif field == "name" or field == "category" or field == "brand" or field == "store_id":
    if operator not in ["contains", "not contains", "="]:
        print(f"Invalid operator for {field}: {operator}. Valid operators are: contains, not contains, =")
        sys.exit(1)

producer.send('ban_rules', value=json.dumps(rule).encode('utf-8'))

print(f"Rule added: {rule}")
producer.flush()