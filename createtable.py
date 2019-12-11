import psycopg2

# Connection to database
try:
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="test",
        port="5432"
    )

    cursor = conn.cursor()

# Make a table
    create_table_query = '''CREATE TABLE ratings(
       rating INT PRIMARY KEY NOT NULL,
       movie_id INT NOT NULL,
       timestamp TIMESTAMP,
       user_id INT NOT NULL
        ); '''

    cursor.execute(create_table_query)
    conn.commit()
    print('Table created')

#Throw error 
except (Exception, psycopg2.DatabaseError) as error:
    print("Error with the database", error)

# Close connection
finally:
    if(conn):
        cursor.close()
        conn.close()
        print('Connection lost')