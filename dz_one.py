import pandas as pd
import sqlite3


class DataHandler:
    def __init__(self, json_file, db_name):
        self.json_file = json_file
        self.db_name = db_name
        self.data = None
        self.conn = None

    def read_json(self):
        # Чтение файла JSON с помощью pandas
        self.data = pd.read_json(self.json_file)

    def write_to_db(self):
        # Запись в базу данных с помощью pandas и sqlite3
        self.conn = sqlite3.connect(self.db_name)
        self.data.to_sql('okved', self.conn, if_exists='replace', index=False)

    def close_connection(self):
        # Закрытие соединения с базой данных
        if self.conn is not None:
            self.conn.close()


# Использование класса
if __name__ == "__main__":
    # Создание объекта DataHandler
    data_handler = DataHandler(json_file='okved_2.json', db_name='hw1.db')

    # Чтение файла JSON
    data_handler.read_json()

    # Запись в базу данных
    data_handler.write_to_db()

    # Закрытие соединения с базой данных
    data_handler.close_connection()
