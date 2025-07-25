from fastapi import FastAPI
import pandas as pd
import psycopg2

app = FastAPI()

connection = psycopg2.connect(database="postgres", user="postgres.kkxxfnciymnwwjwsbdrs", password="Randhawa_07", host="aws-0-eu-west-2.pooler.supabase.com", port="5432")

def execute_query(sql: str):
    try:
        cursor = connection.cursor()
        print(sql)
        cursor.execute(sql)

        # Fetch all rows from database
        record = cursor.fetchall()
        return record
    except Exception as e:
        print(e)
    finally:
        if cursor:
            cursor.close()

def handle_get_customer(name: str): 
    db_query = f"SELECT * FROM database.customer WHERE customer_name = '{name}'"
    data = execute_query(db_query)
    return data


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get('/customers')
async def get_customers(name: str):
    return handle_get_customer(name)

def handle_get_products(product_id: int): 
    db_query = f"SELECT * FROM database.product WHERE product_id = '{product_id}'"
    data = execute_query(db_query)
    return data


@app.get('/products')
def get_all_products():
    db_query = f"SELECT * FROM database.product;"
    data = execute_query(db_query)
    return data

@app.get('/product/{product_id}')
def get_product(product_id: int):
    # Query: SELECT * FROM product WHERE product_id = %s# Return single product or 404 error
    return handle_get_products(product_id)

def handle_get_customer_id(customer_id: int): 
    db_query = f"SELECT * FROM database.customer WHERE customer_id = '{customer_id}'"
    data = execute_query(db_query)
    return data

@app.get('/customer_id')
async def get_customer_id(customer_id: int):
    # Query: SELECT * FROM customer WHERE customer_id = %s
    return handle_get_customer_id(customer_id)



from pydantic import BaseModel

class CustomerCreate(BaseModel):
    customer_name: str
    email: str
    phone_number: str
    address_line_1: str
    city: str

@app.post("/customers")
def create_customer(customer: CustomerCreate):
    # Query: INSERT INTO customer (customer_name, email, phone_number, address_line_1, city) #        VALUES (%s, %s, %s, %s, %s)# Return the created customer with new ID
    return data

@app.put("/customers/{customer_id}")
def update_customer(customer_id: int, customer: CustomerCreate):
    # Query: UPDATE customer SET customer_name=%s, email=%s, phone_number=%s, #        address_line_1=%s, city=%s WHERE customer_id=%s# Return updated customer or 404 if not found
    pass


@app.get("/orders/{order_id}")
def get_order_details(order_id: int):
    # Query: SELECT o.order_id, o.order_date, o.total_amount, #               c.customer_name, c.email, s.status_name#        FROM orders o#        JOIN customer c ON o.customer_id = c.customer_id#        JOIN order_status s ON o.order_status_id = s.order_status_id#        WHERE o.order_id = %s
    pass


@app.get("/orders/{order_id}/items")
def get_order_items(order_id: int):
    # Query: SELECT ol.quantity, p.product_name, p.selling_price,#               (ol.quantity * p.selling_price) as line_total#        FROM order_line ol#        JOIN product p ON ol.product_id = p.product_id#        WHERE ol.order_id = %s
    pass

@app.get("/customers/{customer_id}/orders")
def get_customer_orders(customer_id: int):
    # Query: SELECT o.order_id, o.order_date, o.total_amount, s.status_name#        FROM orders o#        JOIN order_status s ON o.order_status_id = s.order_status_id#        WHERE o.customer_id = %s#        ORDER BY o.order_date DESC
    pass


@app.put("/orders/{order_id}/status")
def update_order_status(order_id: int, new_status_id: int):
    # Query: UPDATE orders SET order_status_id = %s WHERE order_id = %s# Return updated order with status name
    pass


@app.delete("/orders/{order_id}")
def delete_order(order_id: int):
    # First delete order_line items: DELETE FROM order_line WHERE order_id = %s# Then delete order: DELETE FROM orders WHERE order_id = %s# Return success message
    pass