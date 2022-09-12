
# Parallel Query Executor
# Executes MySQL queries in parallel

import mysql.connector
import concurrent.futures
from threading import Lock
from typing import List, Dict
from uuid import uuid4


# global lock
lock = Lock()


class ParallelQueryExecutor:
    """
    Parallel Query Executor
    Executes MySql query in parallel
    """

    def __init__(
        self,
        db_host: str,
        db_user: str,
        db_pass: str,
        db: str,
        payload: List[Dict],
        max_workers: int = 7,
        auto_commit: bool = False,
        close_conn: bool = True) -> None:

        self.db_host: str = db_host
        self.db_user: str = db_user
        self.db_pass: str = db_pass
        self.db: str = db
       
        global lock
        self.lock = lock

        # sample payload
        # payload = {
        # "query": "select * from cards",
        # "data": data}
        self.payload = payload

        self.tasks: dict = {}

        self.max_workers: int = max_workers
        self.auto_commit: bool = auto_commit
        self.close_conn: bool = close_conn
        
    
    def _get_connection(self):
        """
        Returns a db connection
        """
        conn = mysql.connector.connect(
            host=self.db_host,
            user=self.db_user,
            password=self.db_pass,
            database=self.db,
            auth_plugin='mysql_native_password'
        )
        if not self.auto_commit:
            conn.autocommit = False
        return conn

    def _get_cursor(self, conn):
        """
        Returns a cursor for the connection
        """
        if conn:
            return conn.cursor()
        return None
    
    def _is_delete(self, query) -> bool:
        """
        Checks if a query is delete
        """
        return 'delete' in query.lower()
    
    def _is_select(self, query) -> bool:
        """
        Checks if a query is select
        """
        return 'select' in query.lower()
    
    def _is_insert_or_update(self, query) -> bool:
        """
        Checks if a query is insert or update
        """
        return 'insert' in query.lower() or 'update' in query.lower()
    
    def execute_query(self, payload) -> bool:
        """
        Executes a query based on the payload
        Returns -> Bool
        """
        global lock
        
        _id = uuid4()
        
        query = payload.get('query', None)
        data = payload.get('data', None)
        
        if not query:
            lock.acquire()
            self.tasks[_id] = {
                "connection": None,
                "query": None,
                "status": False,
                "error_msg": "No query string specified"
            }
            lock.release()
            return False

        conn = self._get_connection()
        cursor = self._get_cursor(conn)

        if not conn:
            self.tasks[_id] = {
                "connection": None,
                "status": False,
                "error_msg": "Unable to establish connection"
            }
            return False

        # save query and connection object        
        lock.acquire()
        self.tasks[_id] = {
            "connection": conn,
            "query": query
        }
        lock.release()
        
        try:
            rows = None

            if self._is_select(query) and query != None:
                cursor.execute(query)
                rows = cursor.fetchall()
            
            elif self._is_delete(query) and query != None:
                cursor.execute(query)
                if self.auto_commit:
                    conn.commit()

            elif self._is_insert_or_update(query) and \
                query != None and data != None:
                cursor.execute(query, data)
                if self.auto_commit:
                    conn.commit()
            
            # saving the final status, and result if any
            lock.acquire()
            self.tasks[_id]['status'] = True
            if rows:
                self.tasks[_id]['result'] = rows
            else:
                self.tasks[_id]['result'] = None
            lock.release()

        except Exception as ex:
            lock.acquire()
            self.tasks[_id]['status'] = False
            self.tasks[_id]['error_msg'] = str(ex)
            lock.release()
            conn.close()
            return False

        finally:
            if self.close_conn:
                conn.close()

        return True
    
    def runall(self):
        """
        Executes queries in parallel
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            self.results = executor.map(self.execute_query, self.payload)


    

