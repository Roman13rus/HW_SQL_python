import psycopg2
from psycopg2 import Error

class WorkingWithData(): #Класс для работы с данными из БД
    def __init__(self, database, password, user='postgres'):
        self.database = database
        self.password = password
        self.user = user

    def connection_db(self): #Метод обеспечивающий подключение к БД по атрибутам класса
        try:
            self.conn = psycopg2.connect(database=self.database, password=self.password, user=self.user)
            self.cur = self.conn.cursor() 
            self.cur.execute("SELECT version();")
            record = self.cur.fetchone()
            print("Вы подключены к - ", record, "\n")
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
    
    def create_table(self):  # Метод обеспечивающий создание таблиц в БД
        try:
            self.connection_db()
            self.cur.execute("""
                    CREATE TABLE IF NOT EXISTS Client(
                        client_id SERIAL PRIMARY KEY,
                        client_name VARCHAR(40),
                        client_surname VARCHAR(40),
                        client_email VARCHAR(20) UNIQUE
                    );
                    """)
            self.cur.execute("""
                    CREATE TABLE IF NOT EXISTS Client_phone(
                        number_id SERIAL PRIMARY KEY,
                        number VARCHAR(20),
                        client_id INTEGER NOT NULL REFERENCES Client(client_id)
                    );
                    """)
            self.conn.commit() 
            self.cur.execute("""SELECT table_name FROM information_schema.tables
                                WHERE table_schema='public' AND table_type='BASE TABLE'
                                """);
            table = self.cur.fetchmany(2)
            print(f'Созданы таблицы: {table[0]}, {table[1]}')
            return table
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if self.conn:
                self.cur.close()
                self.conn.close()
                print("Соединение с PostgreSQL закрыто")
    def insert_new_client(self, data):   # Метод обеспечивающий добавление элементов в БД(на вход принимает список кортежей, тел.номера вложены в кортеж списком)
        try:
            self.connection_db()
            for i in range(len(data)):
                self.cur.execute("""
                        INSERT INTO Client(client_name, client_surname, client_email) VALUES (%s, %s, %s)""",(data[i][0],data[i][1],data[i][2],))
                if len(data[i][3]) > 0:
                    for num in data[i][3]:
                        self.cur.execute(""";
                            INSERT INTO Client_phone(number, client_id) VALUES (%s, %s)""",(num, i+1,));
                else:
                    self.cur.execute("""
                        INSERT INTO Client_phone(number, client_id) VALUES (%s, %s)""",('NULL', i+1,));
            self.conn.commit()
            print(f'В базу данных внесено {len(data)} записей клиентов.')
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if self.conn:
                self.cur.close()
                self.conn.close()
                print("Соединение с PostgreSQL закрыто")

    def add_tephone_number(self, telephone_num, client_id):  #Метод добавления тел. к клиенту
        try:
            self.connection_db()
            self.cur.execute("""SELECT client_id FROM Client_phone WHERE number=%s;
                            """, ("NULL",))  
            if client_id in self.cur.fetchall():
                self.cur.execute("""
                           UPDATE Client_phone SET number=%s WHERE client_id=%s;
                """, (telephone_num, client_id))
            else:
                self.cur.execute("""
                            INSERT INTO Client_phone(number, client_id) VALUES (%s, %s) """,(telephone_num, client_id,))
            self.conn.commit()
            print(f'Для клиента client_id = {client_id} добавлен номер телефона {telephone_num}.')
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if self.conn:
                self.cur.close()
                self.conn.close()
                print("Соединение с PostgreSQL закрыто")
    def update_client(self,client_id, update_param): #Метод изменения данных клиента(на вход:id клиента и словарь(Ключи-изменяемый параметр, значения - новые данные))
        try:
            self.connection_db()
            data = list(update_param.items())
            for i in range(len(data)):
                if data[i][0] == 'client_name':
                    self.cur.execute("""
                            UPDATE Client SET client_name=%s WHERE client_id=%s;""",(data[i][1],client_id))
                elif data[i][0] == 'client_surname':
                    self.cur.execute("""
                            UPDATE Client SET client_surname=%s WHERE client_id=%s;""",(data[i][1],client_id)) 
                elif data[i][0] == 'client_email':
                    self.cur.execute("""
                            UPDATE Client SET client_email=%s WHERE client_id=%s;""",(data[i][1],client_id)) 
            self.conn.commit()
            self.cur.execute("""SELECT client_name, client_surname, client_email, number FROM Client c
                                        JOIN client_phone cp ON   c.client_id = cp.client_id
                                            WHERE cp.client_id = %s;""", (client_id,))
            new_data = self.cur.fetchone()
            print(f'Новые данные пользователя Имя:{new_data[0]}, Фамилия: {new_data[1]}, email: {new_data[2]}, номер телефона:{new_data[3]}')
            return new_data
            
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if self.conn:
                self.cur.close()
                self.conn.close()
                print("Соединение с PostgreSQL закрыто")
    def del_tephone_number(self, del_number):   #Метод удаления указанного номера телефона
        try:
            self.connection_db()
            self.cur.execute("""SELECT number_id, client_id FROM Client_phone WHERE number=%s;
                            """, (del_number,))
            number_id,client_id = self.cur.fetchone()
            self.cur.execute("""SELECT number_id FROM Client_phone WHERE client_id=%s;
                            """, (client_id,))
            num = len(self.cur.fetchall())
            if num == 1:
                self.cur.execute("""DELETE FROM Client_phone WHERE number_id=%s;
                                    """, (number_id,))
                self.cur.execute("""
                            INSERT INTO Client_phone(number, client_id) VALUES (%s, %s)""",('NULL', client_id,));
            elif num > 1:
                self.cur.execute("""DELETE FROM Client_phone WHERE number_id=%s;
                                    """, (number_id,))
            self.cur.execute("""SELECT client_name, client_surname FROM Client WHERE client_id=%s;
                                """, (client_id,))   
            client_name, client_surname = self.cur.fetchone()
            self.conn.commit()
            
            return f'У клиента {client_name} {client_surname} удален номер телефона {del_number}'
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if self.conn:
                self.cur.close()
                self.conn.close()
                print("Соединение с PostgreSQL закрыто")
    def del_client(self, client_id): # Метод удаления всех данных о клиенте
        try:
            self.connection_db()
            self.cur.execute("""SELECT client_name, client_surname FROM Client WHERE client_id=%s;
                            """, (client_id,))
            client_name, client_surname = self.cur.fetchone()
            result = input("Вы действительно хотите удалить данные клиента {client_name} {client_surname}? (Да/Нет)")
            if result.lower() == 'да':
                self.cur.execute("""DELETE FROM Client_phone WHERE client_id=%s;
                                """, (client_id,))
                self.cur.execute("""DELETE FROM Client WHERE client_id=%s;
                                """, (client_id,))
                self.conn.commit()
                return f"Данные клиента {client_name} {client_surname} удалены."
            else:
                return f"Данные клиента {client_name} {client_surname} сохранены в базе без изменений."
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if self.conn:
                self.cur.close()
                self.conn.close()
                print("Соединение с PostgreSQL закрыто")
    
    def get_data_client(self, data:tuple): #Метод получения данных о клиенте(на вход кортеж (перое значение - параметр, второе бего значение))
        try:
            self.connection_db()
            if data[0] == 'client_name':
                self.cur.execute("""SELECT c.client_id, client_name, client_surname, client_email, cp.number FROM Client c
                                    JOIN client_phone cp ON c.client_id = cp.client_id
                                    WHERE c.client_name = %s;
                                """, (data[1],))
                # return self.cur.fetchall()
            elif data[0] == 'client_surname':
                self.cur.execute("""SELECT c.client_id, client_name, client_surname, client_email, cp.number FROM Client c
                                    JOIN client_phone cp ON c.client_id = cp.client_id
                                    WHERE c.client_surname = %s;
                                """, (data[1],))
                # return self.cur.fetchall()
            
            elif data[0] == 'client_email':
                self.cur.execute("""SELECT c.client_id, client_name, client_surname, client_email, cp.number FROM Client c
                                    JOIN client_phone cp ON c.client_id = cp.client_id
                                    WHERE client_email=%s;
                                """, (data[1],))
                # return self.cur.fetchall()
            elif data[0] == 'client_id':
                self.cur.execute("""SELECT c.client_id, client_name, client_surname, client_email, cp.number FROM Client c
                                    JOIN client_phone cp ON c.client_id = cp.client_id
                                    WHERE c.client_id = %s;
                                """, (data[1],))
                # return self.cur.fetchall()
            elif data[0] == 'number':
                self.cur.execute("""SELECT c.client_id, client_name, client_surname, client_email, number FROM Client c
                                    JOIN client_phone cp ON c.client_id = cp.client_id
                                    WHERE cp.number = %s;
                                """, (data[1],))
            client_data = self.cur.fetchall()
            for d in client_data:
                print(f'Клиент Номер: {d[0]}, Имя: {d[1]}, Фамилия: {d[2]}, email: {d[3]}, Телефонный номер: {d[4]}')
            return client_data
    
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if self.conn:
                self.cur.close()
                self.conn.close()
                print("Соединение с PostgreSQL закрыто")

wor1 = WorkingWithData('Client_info',  '410232qwerty') # Создание экземпляра класса подключения к БД
# wor1.create_table() # Создание таблиц
data = [('Роман', 'Иванов', 'hihi@rambler.ru', ['89003456787','567987']), ('Иван', 'Стерлигов', 'primer@ryt.ro',['88998659498']), ('Петр','Петров', 'pertov39@werty.com', ['678939548785','89030848404','276364747']), ('Иван','Иванов', 'ivanov@mail.com', [])]
# wor1.insert_new_client(data) #Заполнение таблиц
# wor1.add_tephone_number('1235677456',4)  #Добавление номера телефона клиенту
# wor1.add_tephone_number('3456787654',2)  #Добавление номера телефона клиенту
update_param = {'client_name':'Вася',  'client_email':'8765544@mail.py'} # пример данных на изменение в def update_client()
update_param1 = {'client_email':'newemail@rambler.ru'}   # пример данных на изменение в def update_client()
# wor1.update_client(1, update_param)   # Изменение данных о клиенте
# wor1.update_client(3, update_param1)   # Изменение данных о клиенте
# print(wor1.del_tephone_number('89003456787'))  #Удаление номера телефона
# print(wor1.del_client(3))   # Удаление данных о клиенте
data = ('number', '1235677456')
data1 = ('client_surname', 'Стерлигов')
data2 = ("client_email", "ivanov@mail.com")
data3 = ('client_name', 'Иван')
# print(wor1.get_data_client(data))  # Получение данных о клиенте
# print(wor1.get_data_client(data1))  # Получение данных о клиенте
# print(wor1.get_data_client(data2))  # Получение данных о клиенте
# print(wor1.get_data_client(data3))  # Получение данных о клиенте