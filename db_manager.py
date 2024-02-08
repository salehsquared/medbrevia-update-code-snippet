import mysql.connector
from threading import Lock

from utils import Utils


class SQLManager:
    def __init__(self):
        self.config = {
            'user': 'root',
            'host': '',
            'database': '',
            'ssl_disabled': False
        }

        self.connection = mysql.connector.connect(**self.config)
        Utils.print("Connected to MySQL database.")
        self.cursor = self.get_cursor()
        self.summaries_queue = []
        self.summaries_queue_batch_size = 30
        self.lock = Lock()

    def get_cursor(self):
        if self.connection.is_connected():
            return self.connection.cursor()
        self.connection.reconnect()
        self.connection.autocommit = True
        return self.connection.cursor()

    def get_sql_values_string_from_dicts(self, arr_of_dicts: list[dict], ordering):
        base_string = " VALUES "
        num_fields = len(ordering)
        format_string = "(" + ("%s," * num_fields)[:-1] + "),"
        values = []

        for d in arr_of_dicts:
            new_value = []
            for key in ordering:
                if key in d:
                    new_value.append(d[key] if d[key] != '' else None)
                else:
                    new_value.append(None)
            base_string += format_string
            values.extend(new_value)

        return base_string[:-1], values
    
    def pull_for_elasticsearch(self, min_pubmed_id:int, count:int=50):
        query = """
        SELECT * FROM pubmed_articles
        WHERE pubmed_id >= %s
        ORDER BY pubmed_id ASC
        LIMIT %s;
        """
        self.cursor.execute(query, (min_pubmed_id, count))
        rows = self.cursor.fetchall()
        
        # Assuming you want to return a list of dictionaries
        columns = [desc[0] for desc in self.cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
        return result
    def push_pubmed_articles(self, pubmed_articles: list[dict]):
        try:
            article_sql, values = self.get_sql_values_string_from_dicts(pubmed_articles, ['pubmed_id', 'title', 'pub_date', 'doi', 'journal', 'nlm_unique_id', 'pub_types', 'abstract'])
            insert_sql = "INSERT INTO pubmed_articles (pubmed_id, title, pub_date, doi, journal, nlm_unique_id, pub_types, abstract)" + article_sql
            update_sql = " ON DUPLICATE KEY UPDATE title=VALUES(title), pub_date=VALUES(pub_date), doi=VALUES(doi), journal=VALUES(journal), nlm_unique_id=VALUES(nlm_unique_id), pub_types=VALUES(pub_types), abstract=VALUES(abstract), last_update=NOW()"
            insert_sql += update_sql
            self.cursor.execute(insert_sql, values)
            self.connection.commit()
            return True
        except Exception as e:
            Utils.print(f"Error in push_pubmed_articles: {e}")
            return False

    def write_file_names(self, file_names: list[str]) -> bool:
        try:
            values = [(name,) for name in file_names]
            insert_sql = "INSERT INTO indexed_pubmed_files (file_name) VALUES (%s) ON DUPLICATE KEY UPDATE last_update=NOW()"
            self.cursor.executemany(insert_sql, values)
            self.connection.commit()
            return True
        except Exception as e:
            Utils.print(f"Error in write_file_names: {e}")
            return False
        
    def write_file_name(self, file_name: str) -> bool:
        return self.write_file_names([file_name])
        
    def get_indexed_pubmed_files(self):
        query = "SELECT file_name FROM indexed_pubmed_files"
        self.cursor.execute(query)
        result = [item[0] for item in self.cursor.fetchall()]
        return result