import psycopg2

## Bu değeri localinde çalışırken kendi passwordün yap. Ama kodu pushlarken 'postgres' olarak bırak.
password = '1234' 

def connect_db():
    conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password=password)
    return conn

# 1. customers tablosundan tüm müşterilerin adlarını ve ülkelerini getir.
def question_1_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT customer_name, country FROM test.customers;')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# 2. orders tablosunda en yüksek tutarlı 5 siparişi listele (tüm sütunlarla birlikte).
def question_2_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM test.orders ORDER BY total_amount DESC LIMIT 5;')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# 3. products tablosundan fiyatı en düşük 3 ürünü, sadece adları ve fiyatları ile getir.
def question_3_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT product_name, price FROM test.products ORDER BY price ASC LIMIT 3;')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# 4. customers tablosundaki en eski kayıtlı 10 müşteriyi signup_date'e göre sırala.
def question_4_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM test.customers ORDER BY signup_date ASC LIMIT 10;')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# 5. products tablosunda en fazla stoğa sahip ürünü sadece adı ve stock_quantity ile getir.
def question_5_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT product_name, stock_quantity FROM test.products ORDER BY stock_quantity DESC LIMIT 1;')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# 6. orders tablosunda son siparişi (tarihi en güncel olanı) listele.
def question_6_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM test.orders ORDER BY order_date DESC LIMIT 1;')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# 7. products tablosunda sadece product_name sütununu alfabetik sırada göster.
def question_7_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT product_name FROM test.products ORDER BY product_name ASC;')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# 8. customers tablosundan email sütunu olan ilk 5 müşteriyi customer_id'ye göre sırala.
def question_8_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT customer_id, email FROM test.customers WHERE email IS NOT NULL ORDER BY customer_id ASC LIMIT 5;')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# 9. orders tablosunda tutarı en düşük 3 siparişi ve bunların order_id'lerini getir.
def question_9_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT order_id, total_amount FROM test.orders ORDER BY total_amount ASC LIMIT 3;')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# 10. customers tablosundan sadece Türkiye'deki (country = 'Turkey') müşterileri alfabetik sırala.
def question_10_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM test.customers WHERE country = 'Turkey' ORDER BY customer_name ASC;")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data