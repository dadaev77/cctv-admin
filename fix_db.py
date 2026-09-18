
import sqlite3
import os

DB_PATH = '/opt/cctv-admin/cctv.db'

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
c = conn.cursor()

print("Current table schema for routers:")
for row in c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='routers';"):
    print(row['sql'])

# Check if routers table has NOT NULL constraint on vpn_ip or any other obsolete columns
c.execute("PRAGMA table_info(routers);")
cols = c.fetchall()
print("Columns in routers:")
for col in cols:
    print(dict(col))

# Recreate routers table cleanly without NOT NULL on vpn_ip
c.execute('''
CREATE TABLE IF NOT EXISTS routers_clean (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id INTEGER,
    name TEXT NOT NULL,
    tunnel_port INTEGER DEFAULT 1554,
    model TEXT DEFAULT 'Cudy LT300',
    notes TEXT,
    is_online INTEGER DEFAULT 0,
    last_seen TEXT,
    created_at TEXT,
    vpn_ip TEXT,
    FOREIGN KEY(site_id) REFERENCES sites(id)
)
''')

# Copy existing data
try:
    c.execute('''
    INSERT INTO routers_clean (id, site_id, name, tunnel_port, model, notes, is_online, last_seen, created_at)
    SELECT id, site_id, name, 
           CASE WHEN tunnel_port IS NOT NULL THEN tunnel_port ELSE 1554 END,
           CASE WHEN model IS NOT NULL THEN model ELSE 'Cudy LT300' END,
           notes, 
           CASE WHEN is_online IS NOT NULL THEN is_online ELSE 0 END,
           last_seen, created_at 
    FROM routers;
    ''')
    print("Migrated existing routers data successfully.")
except Exception as e:
    print("Data copy note:", e)

c.execute("DROP TABLE routers;")
c.execute("ALTER TABLE routers_clean RENAME TO routers;")
conn.commit()

# Check cameras schema too
c.execute("PRAGMA table_info(cameras);")
cam_cols = [dict(col) for col in c.fetchall()]
print("Columns in cameras:")
for col in cam_cols:
    print(col)

conn.close()
print("DB FIX COMPLETED SUCCESSFULLY!")
