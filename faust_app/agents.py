from app import app
from topics import products_topic, filtered_products_topic, ban_rules_topic, products_serializer
import logging

logger = logging.getLogger(__name__)
ban_rules = []

# --- Business Logic: Product Ban Check ---
def is_product_banned(product: dict) -> bool:
    """Checks the product against the list of rules."""
    if not isinstance(product, dict):
        logger.error(f"Invalid product format in is_product_banned: {type(product)}")
        return False # Can't check passing

    product_name = product.get('name', 'N/A')
    logger.debug(f"Checking product '{product_name}' against {len(ban_rules)} rules.")

    for i, rule in enumerate(ban_rules):
        field = rule['field']
        operator = rule['operator']
        rule_value = rule['value']
        rule_currency = rule.get('currency')

        product_value = product.get(field)
        logger.debug(f"Rule {i+1}: Field='{field}', Operator='{operator}', RuleValue='{rule_value}'")

        if product_value is None:
            logger.debug(f"Rule {i+1}: Product field '{field}' is None, skipping.")
            continue

        try:
            match_found = False
            if field == 'price':
                if not isinstance(product_value, dict) or 'amount' not in product_value or 'currency' not in product_value:
                    logger.warning(f"Rule {i+1}: Product 'price' format invalid: {product_value}. Skipping.")
                    continue
                product_amount = product_value.get('amount')
                product_currency = product_value.get('currency')

                if rule_currency != product_currency: # Compare only the same currency
                    logger.debug(f"Rule {i+1}: Currency mismatch (Rule: {rule_currency}, Prod: {product_currency}). Skipping.")
                    continue
                if not isinstance(product_amount, (int, float)):
                    logger.warning(f"Rule {i+1}: Product price amount invalid: {product_amount}. Skipping.")
                    continue

                # Price comparison (rule_value уже float)
                logger.debug(f"Rule {i+1}: Comparing price: Prod={product_amount}, Op='{operator}', Rule={rule_value}")
                if operator == '>' and product_amount > rule_value: match_found = True
                elif operator == '<' and product_amount < rule_value: match_found = True
                elif operator == '=' and product_amount == rule_value: match_found = True

            elif field in ["name", "category", "brand", "store_id"]:
                if not isinstance(product_value, str):
                    logger.warning(f"Rule {i+1}: Product field '{field}' not string: {product_value}. Skipping.")
                    continue
                product_str_value = product_value.lower() # Compare i lower registry
                logger.debug(f"Rule {i+1}: Comparing string: Prod='{product_str_value}', Op='{operator}', Rule='{rule_value}'")
                if operator == 'contains' and rule_value in product_str_value: match_found = True
                elif operator == 'not contains' and rule_value not in product_str_value: match_found = True
                elif operator == '=' and product_str_value == rule_value: match_found = True

            if match_found:
                logger.info(f"Product '{product_name}' (ID: {product.get('product_id', 'N/A')}) BLOCKED by rule {i+1}: {rule}")
                return True # Banned

        except Exception as e:
            logger.error(f"Error comparing product with rule {i+1} ({rule}): {e}. Product: {str(product)[:200]}", exc_info=True)

    logger.debug(f"Product '{product_name}' PASSED all rules.")
    return False # Passed

# --- Business Logic: Product Processing Agent ---
@app.agent(products_topic)
async def process_products(products):
    async for product in products:
        if product:
            logger.info(f"Product: {product}")
            deserialized_product = products_serializer.loads(product)
            if deserialized_product:
                logger.info(f"Successfully deserialized product: {deserialized_product}")

                if not isinstance(product, dict):
                    logger.error(f"Received non-dict after deserialization: {type(product)} - {product}")
                    continue

                product_name = deserialized_product.get('name', 'N/A')
                product_id = deserialized_product.get('product_id', 'N/A')
                logger.info(f"Processing product: Name='{product_name}', ID='{product_id}'")

                try:
                    if not is_product_banned(deserialized_product):
                        logger.info(f"Product '{product_name}' PASSED. Sending to filtered topic.")
                        await filtered_products_topic.send(value=product)
                except Exception as e:
                    logger.error(f"Error processing product '{product_name}' (ID: {product_id}): {e}", exc_info=True)


# --- Business Logic: Ban Rules Agent ---
@app.agent(ban_rules_topic)
async def load_ban_rules(stream):
    """Loads and validates the ban rules."""
    global ban_rules
    async for rule in stream:
        logger.debug(f"Received potential ban rule: {rule}")
        try:
            field = rule.field
            operator = rule.operator
            value = rule.value
            currency = rule.currency

            if field not in ["price", "name", "category", "brand", "store_id"]:
                logger.error(f"Invalid 'field': {field}. Rule: {rule}")
                continue
            if operator not in [">", "<", "=", "contains", "not contains"]:
                logger.error(f"Invalid 'operator': {operator}. Rule: {rule}")
                continue
            if value is None:
                logger.error(f"'value' is missing. Rule: {rule}")
                continue

            if field == "price":
                if not currency:
                    logger.error(f"'currency' required for price rule: {rule}")
                    continue
                if operator not in [">", "<", "="]:
                    logger.error(f"Invalid operator '{operator}' for 'price'. Rule: {rule}")
                    continue
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    logger.error(f"Invalid numeric 'value' for price: '{value}'. Rule: {rule}")
                    continue
            elif field in ["name", "category", "brand", "store_id"]:
                if operator not in ["contains", "not contains", "="]:
                    logger.error(f"Invalid operator '{operator}' for string field '{field}'. Rule: {rule}")
                    continue
                if not isinstance(value, str):
                    logger.error(f"Invalid string 'value' for '{field}': '{value}'. Rule: {rule}")
                    continue
                value = value.lower()

            prepared_rule = {
                "field": field,
                "operator": operator,
                "value": value,
                "currency": currency
            }

            ban_rules.append(prepared_rule)
            logger.info(f"Ban rule added: {prepared_rule}. Total rules: {len(ban_rules)}")

        except Exception as e:
            logger.error(f"Error processing rule: {rule}. Error: {e}", exc_info=True)
