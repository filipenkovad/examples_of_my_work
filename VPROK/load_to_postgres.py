import pandas as pd
from datetime import date
from sqlalchemy import MetaData, Table, Column, String, Integer, Float, Date, text, create_engine

#Считаем файл
data = pd.read_csv('LTV.csv', parse_dates=['Effective To Date'])
#Преобразуем столбец с датой в string, для корректной записи в БД
data['Effective To Date'] = data['Effective To Date'].astype(str)
#Создадим строку подключения к БД Postgres
conn_string = 'postgresql+psycopg2://postgres:postgres@localhost:5433/postgres'
engine = create_engine(conn_string)
#Создадим таблицу 'ltv'
meta = MetaData()
ltv = Table(
    'LTV', meta,
    Column('Customer', String, nullable=False),
    Column('State', String),
    Column('Customer Lifetime Value', Float),
    Column('Response', String),
    Column('Coverage', String),
    Column('Education', String),
    Column('Effective To Date', Date),
    Column('EmploymentStatus', String),
    Column('Gender', String),
    Column('Income', Integer),
    Column('Location Code', String),
    Column('Marital Status', String),
    Column('Monthly Premium Auto', Integer),
    Column('Months Since Last Claim', Integer),
    Column('Months Since Policy Inception', Integer),
    Column('Number of Open Complaints', Integer),
    Column('Number of Policies', Integer),
    Column('Policy Type', String),
    Column('Policy', String),
    Column('Renew Offer Type', String),
    Column('Sales Channel', String),
    Column('Total Claim Amount', Float),
    Column('Vehicle Class', String),
    Column('Vehicle Size', String),
    Column('Load Date', Date)
)
meta.create_all(engine)

#Подключимся к БД
conn = engine.connect()
#Добавим столбец Load Date
data['Load Date'] = str(date.today())
#Запишем данные датафрейма в таблицу 'ltv', которую мы создали
data.to_sql('ltv', con=conn, if_exists='replace', index=False)
#Закроем подключение к БД
conn.close()
