import os
import psycopg2
import random
from datetime import datetime

def run():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            port=os.getenv('DB_PORT')
        )
        cur = conn.cursor()
        print("✅ Conexión establecida.")

        # --- 1. CRECIMIENTO AGRESIVO (10 a 20 usuarios nuevos) ---
        nuevos_count = random.randint(10, 20)
        for _ in range(nuevos_count):
            id_random = random.randint(1000, 9999)
            cur.execute("""
                INSERT INTO users (name, email, country, tier, status, signup_date)
                VALUES (%s, %s, %s, %s, 'Active', CURRENT_DATE)
            """, (
                f"User_New_{id_random}",
                f"grow_{id_random}_{datetime.now().strftime('%M%S')}@fintech.com",
                random.choice(['Colombia', 'Mexico', 'Chile', 'Argentina', 'Peru']),
                random.choice(['Free', 'Premium'])
            ))

        # --- 2. ACTIVIDAD DE TRANSACCIONES (Damos vida a 100 usuarios) ---
        cur.execute("SELECT user_id FROM users WHERE status = 'Active' ORDER BY random() LIMIT 100")
        user_ids = [row[0] for row in cur.fetchall()]
        for uid in user_ids:
            cur.execute("""
                INSERT INTO transactions (user_id, amount, transaction_type, status, merchant_category, transaction_date)
                VALUES (%s, %s, %s, 'Success', %s, CURRENT_TIMESTAMP)
            """, (
                uid,
                round(random.uniform(5, 500), 2),
                random.choice(['Pago', 'Transferencia', 'Recarga']),
                random.choice(['Food', 'Transport', 'Utilities', 'Entertainment', 'Health'])
            ))

        # --- 3. GESTIÓN DE RETIROS (CHURN) ---
        # 3 usuarios al azar deciden dejar la app hoy
        cur.execute("""
            UPDATE users SET status = 'Inactive' 
            WHERE user_id IN (SELECT user_id FROM users WHERE status = 'Active' ORDER BY random() LIMIT 3)
        """)

        # --- 4. UPGRADES (FIDELIZACIÓN) ---
        # 2 usuarios suben a Gold hoy
        cur.execute("""
            UPDATE users SET tier = 'Gold' 
            WHERE user_id IN (SELECT user_id FROM users WHERE tier != 'Gold' AND status = 'Active' ORDER BY random() LIMIT 2)
        """)

        conn.commit()
        cur.close()
        conn.close()
        print(f"🚀 Éxito: {nuevos_count} nuevos usuarios y 100 transacciones generadas.")

    except Exception as e:
        print(f"❌ Error en el robot: {e}")

if __name__ == "__main__":
    run()
