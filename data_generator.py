import os
import psycopg2

def run():
    try:
        # Conexión
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            port=os.getenv('DB_PORT')
        )
        cur = conn.cursor()
        
        # 1. Insertar 100 transacciones de prueba hoy
        # Usamos una subconsulta para agarrar usuarios existentes
        cur.execute("""
            INSERT INTO transactions (user_id, amount, transaction_type, status, merchant_category, transaction_date)
            SELECT 
                user_id, 
                (random() * 100)::decimal(12,2), 
                'Pago', 
                'Success', 
                'Food', 
                NOW()
            FROM users 
            LIMIT 100;
        """)
        
        # 2. EL PASO MÁS IMPORTANTE: Confirmar los cambios
        conn.commit()
        
        print(f"✅ Se insertaron {cur.rowcount} filas nuevas.")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run()
