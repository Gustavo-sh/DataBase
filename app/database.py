import pyodbc
from queue import Queue, Empty
from contextlib import contextmanager

MAX_POOL_SIZE = 100 
CONNECTION_STRING = "Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;"
#CONNECTION_STRING = "Driver={ODBC Driver 18 for SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;"
_connection_pool = Queue(maxsize=MAX_POOL_SIZE)

def _create_connection():
    return pyodbc.connect(CONNECTION_STRING, timeout=10)

def _populate_pool():
    """Pré-enche o pool com algumas conexões iniciais."""
    print("Preenchendo Pool de Conexões...")
    for _ in range(5):
        try:
            conn = _create_connection()
            _connection_pool.put(conn)
        except Exception as e:
            print(f"Erro ao criar conexão inicial: {e}")
            break
    print(f"Pool de Conexões preenchido: {_connection_pool.qsize()} conexões.")

#_populate_pool() 

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = _connection_pool.get(timeout=1)
    except Empty:
        try:
            conn = _create_connection()
        except Exception as e:
            print(f"Erro fatal ao criar nova conexão: {e}")
            raise
    try:
        yield conn
    finally:
        if conn is not None:
            if not conn.closed:
                try:
                    cur = conn.cursor()
                    cur.execute('SELECT 1')
                    cur.close()
                    _connection_pool.put(conn)
                    
                except pyodbc.Error as e:
                    print(f"Alerta: Conexão ping falhou ({e}). Descartando do pool.")
                    try:
                        conn.close()
                    except:
                        pass
                
            else:
                print("Conexão falhou e foi descartada do pool.")

def init_db_pool():
    _populate_pool()