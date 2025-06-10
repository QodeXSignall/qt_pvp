from sqlalchemy import MetaData, Table, String, Integer, Column, ForeignKey, \
    DateTime, Boolean, Text, DATETIME
import datetime

metadata = MetaData()

settings = Table('settings', metadata,
                 Column('id', Integer(), primary_key=True, autoincrement=True),
                 Column('key', String(), unique=True),
                 Column('value', String())
                 )

clients = Table('clients', metadata,
                Column('id', Integer(), primary_key=True, autoincrement=True),
                Column('name', String(), nullable=False),
                Column('kf', Integer(), nullable=True, default=0),
                Column('ex_id', Integer(), nullable=True, unique=True),
                Column('created_on', DateTime(),
                       default=datetime.datetime.now),
                Column('updated_on', DateTime(), default=datetime.datetime.now,
                       onupdate=datetime.datetime.now))

auto = Table('auto', metadata,
             Column('id', Integer(), primary_key=True, autoincrement=True),
             Column('car_number', String(), nullable=False),
             Column('kf', Integer(), nullable=True, default=0),
             Column('ex_id', Integer(), nullable=True, unique=True),
             Column('created_on', DateTime(),
                    default=datetime.datetime.now),
             Column('updated_on', DateTime(), default=datetime.datetime.now,
                    onupdate=datetime.datetime.now))

kf_sources = Table('kf_sources', metadata,
                   Column('id', Integer(), primary_key=True,
                          autoincrement=True),
                   Column('name', String()),
                   Column('description', String()))

records = Table('records', metadata,
                Column('id', Integer(), primary_key=True, autoincrement=True),
                Column('record_id', Integer()),
                Column('clear_gross', Integer()),
                Column('hermes_gross', Integer()),
                Column('final_gross', Integer()),
                Column('clear_cargo', Integer()),
                Column('hermes_cargo', Integer()),
                Column('final_cargo', Integer()),
                Column('tare', Integer()),
                Column('test_mode', Boolean(), default=False),
                Column('kf_source_id', Integer(), ForeignKey('kf_sources.id')),
                Column('notes', Text()),
                Column('kf_must_be', String(6)))


records = Table