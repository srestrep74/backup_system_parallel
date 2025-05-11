import pandas as pd
from os import path
import os

class DatabaseManager:
    def __init__(self):
        self.db_path = path.join(path.dirname(__file__), '..', '..', 'db.csv')
        try:
            self.db = pd.read_csv(self.db_path)
        except FileNotFoundError:
            self.db = pd.DataFrame(columns=['filename', 'path'])
        
    def list_fragmented_files(self, filename):
        return self.db[self.db['filename'] == filename]['path']
    
    def insert_fragment(self, filename: str, path: str) -> None:
        new_row = pd.DataFrame({'filename': [filename], 'path': [path]})
        self.db = pd.concat([self.db, new_row], ignore_index=True)
        self._save_database()
    
    def _save_database(self):
        try:
            os.makedirs(path.dirname(self.db_path), exist_ok=True)
            self.db.to_csv(self.db_path, index=False)
        except Exception as e:
            raise Exception(f"Failed to save database: {str(e)}")
