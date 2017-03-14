import os
import sys
import numpy
import datetime
import pandas
import time
import pytz
from tzlocal import get_localzone
from sqlalchemy import Column, ForeignKey, Integer, String, Float, DateTime, Time, Interval, UniqueConstraint, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import StaticPool

from swigibpy import Contract as IBcontract

class Base(object):
	@classmethod
	def filter(cls, *args, **kwargs):
		return session.query(cls).filter(*args, **kwargs)

	@classmethod
	def filter_df(cls, *args, **kwargs):
		rows = [r.__dict__ for r in session.query(cls).filter(*args, **kwargs).all()]
		df = pandas.DataFrame(rows)
		columns = cls.__table__.columns.keys()

		try:
			return df.drop('_sa_instance_state', 1)[columns]
		except ValueError:
			if len(df) == 0:
				return pandas.DataFrame({c:[] for c in columns})[columns]
			else:
				return df[columns]

	@classmethod
	def drop(cls):
		try:
			cls.__table__.drop(engine)
		except OperationalError:
			print "Warning: No such table"
		
	def todict(self):
		return {c.name: getattr(self, c.name) for c in self.__table__.columns}
	
	def save(self):
		try:
			session.add(self)
			session.commit()
		except Exception as e:
			print "WARNING"
			print e
	
	def delete(self):
		try:
			session.delete(self)
			session.commit()
		except Exception as e:
			print "WARNING"
			print e
		
	# id = Column(Integer, primary_key=True)

#create base sqlalchemy object
Base = declarative_base(cls=Base)
engine = create_engine('sqlite:///C:/Users/mcherkassky/Desktop/ib/db/data.db',
			connect_args={'check_same_thread':False},
			poolclass=StaticPool)

#create sqlalchemy session
Base.metadata.bind = engine
DBSession = scoped_session(sessionmaker(bind=engine))
session = DBSession()		

	
	
	
	
	
	

