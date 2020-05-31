# -*- coding: utf-8 -*-

from sqlalchemy import Column, Integer, String, \
    Boolean, ForeignKey
from sqlalchemy.types import VARBINARY
from sqlalchemy.orm import relationship
from sqlalchemy import func
from sqlalchemy.sql.expression import true
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from helpers import grouper


Model = declarative_base(name='Model')


class HashColumn(VARBINARY):
    def bind_expression(self, bindvalue):
        return func.UNHEX(bindvalue)

    def column_expression(self, col):
        return func.HEX(col)

class Records(Model):
    __tablename__ = 'records'
    id = Column(Integer, primary_key=True)
    record_name = Column(String(512))
    fingerprinted = Column(Boolean)
    file_sha1 = Column(HashColumn(32))


class Fingerprints(Model):
    __tablename__ = 'fingerprints'
    id = Column(Integer, primary_key=True)
    hash = Column(HashColumn(16))
    offset = Column(Integer)
    record_id = Column(Integer, ForeignKey('records.id'))
    record = relationship("Records", backref='fingerprints')


class Database:

    def __init__(self, user, password, host='localhost', recreate_db=False):
        database_uri = 'mysql+mysqldb://{}:{}@{}'.format(
            user, password, host
        )

        self.engine = create_engine(
            database_uri,
            echo=False
        )

        self.session = self.init_db(recreate_db)

    def init_db(slef, recreate_db=False):
        if recreate_db:
            slef.engine.execute('DROP DATABASE IF EXISTS dejavudb;')
            slef.engine.execute('CREATE DATABASE dejavudb')
        slef.engine.execute("USE dejavudb")
        Model.metadata.create_all(bind=slef.engine)

        return scoped_session(
            sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=slef.engine
            )
        )

    def get_records(self):
        query = self.session.query(
            Records.id,
            Records.record_name,
            Records.file_sha1
        ).filter(
            Records.fingerprinted == true()
        )

        for row in query.all():
            yield row

    def insert_record(self, song_name, file_hash):
        row = Records(
            record_name=song_name,
            fingerprinted=False,
            file_sha1=file_hash.encode('utf-8')
        )

        self.session.add(row)
        self.session.commit()

        return row.id

    def insert_hashes(self, record_id, hashes):
        values = []
        for hash, offset in hashes:
            values.append(
                {
                    'hash': hash,
                    'record_id': record_id,
                    'offset': offset
                }
            )
        rows = []
        for split_values in grouper(values, 1000):
            for row in split_values:
                rows.append(Fingerprints(**row))

        self.session.add_all(rows)
        self.session.commit()

    def set_record_fingerprinted(self, id):
        self.session.query(Records).filter_by(
            id=id
        ).update(
            {"fingerprinted": 1}
        )
        self.session.commit()

    def get_record_by_id(self, id):
        return self.session.query(
            Records.record_name,
            Records.file_sha1
        ).filter_by(
            id=id
        ).first()

    def return_matches(self, hashes):
        mapper = {}
        for hash, offset in hashes:
            mapper[hash.upper()] = offset

        values = mapper.keys()

        for split_values in grouper(values, 1000):
            records = self.session.query(
                Fingerprints.hash,
                Fingerprints.record_id,
                Fingerprints.offset
            ).filter(
                Fingerprints.hash.in_(list(split_values))
            )

            for row in records.all():
                key = bytes("{0}".format(row[0]), encoding="ascii")
                if key in mapper:
                    yield (row[1], row[2] - mapper[key])