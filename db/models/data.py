import os
import sys
import numpy
import datetime
import pandas
import time
import pytz
import Quandl

from tzlocal import get_localzone
from sqlalchemy import Column, ForeignKey, Integer, String, Float, DateTime, Time, Interval, UniqueConstraint, Boolean, and_, create_engine, desc, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError, OperationalError, InvalidRequestError
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from db.sqlite import *
from utils.config import QUANDL_API_KEY
from db.models.trading import *
from utils import convert_EST, fut_date
from utils.mail import Email

FUT_CONTRACT_CODES = {code: month + 1 for month, code in enumerate('FGHJKMNQUVXZ')}
# INTRADAY_PATH = 'C:\\Users\\mcherkassky\\Desktop\\marketdata\\intraday'
INTRADAY_PATH = 'C:\\Users\\mcherkassky\\Desktop\\kibot'

class Heartbeat(Base):
	__tablename__ = 'Heartbeat'

	id = Column(Integer, primary_key=True)
	name = Column(String(250), nullable=False)
	last_seen = Column(DateTime, nullable=False)

	@classmethod
	def initialize(cls):
		heartbeat = pandas.DataFrame({'name': ['Trader', 'OMS'], 'last_seen': [datetime.datetime.now()] * 2})
		heartbeat.to_csv('heart.beat', index=False)

	@classmethod
	def update(cls, name):
		try:
			heartbeat = pandas.read_csv('heart.beat')
		except IOError:
			cls.initialize()

		heartbeat.ix[heartbeat.name == name, 'last_seen'] = datetime.datetime.now()

		heartbeat.to_csv('heart.beat', index=False)

	@classmethod
	def check(cls, threshold = 120):
		try:
			heartbeats = pandas.read_csv('heart.beat')
		except IOError:
			cls.initialize()
		heartbeats.last_seen = pandas.to_datetime(heartbeats.last_seen)

		for i in xrange(len(heartbeats)):
			heartbeat = heartbeats.iloc[i]

			if (datetime.datetime.now() - heartbeat.last_seen).seconds > threshold:
				email = Email("CRITICAL", "{0} Heartbeat Dead".format(heartbeat['name']))
				email.send(['9522206201@txt.att.net'])

class DataManager:
	def __init__(self, df, strategy=None):
		if 'dt' in df:
			df.dt = pandas.to_datetime(df.dt)
		else:
			if len(df) == 0:
				pass
			else:
				raise Exception('No dt column in DataManager DataFrame')

		self.data = df
		self.strategy = strategy

	@property
	def dt(self):
		if self.strategy:
			return self.strategy.dt
		else:
			return fut_date(datetime.datetime.now)

	def today(self):
		return self.data[self.data.dt == fut_date(self.dt)].iloc[0]

	def yesterday(self):
		yest_data = self.data[self.data.dt < fut_date(self.dt)]
		return yest_data.iloc[-1]

	def last(self):
		pass

	def history(self):
		hist_data = self.data[self.data.dt < fut_date(self.dt)]
		return hist_data
'''
class Intraday(Base):
	__tablename__ = 'Intraday'
	
	id = Column(Integer, primary_key=True)
	seriesid = Column(Integer, ForeignKey('Series.id'))
	contractid = Column(Integer, ForeignKey('Contract.id'))
	dt = Column(DateTime, nullable=False)
	open_ = Column(Float, nullable=True)
	high = Column(Float, nullable=True)
	low = Column(Float, nullable=True)
	close_ = Column(Float, nullable=True)
	volume = Column(Integer, nullable=True)

	__table_args__ = (UniqueConstraint('dt', 'contractid'),)

	@classmethod
	def load_file(cls, contractid, path):
		from db.models.trading import Series, Contract
		contract = Contract.filter(Contract.id == contractid).first()
		seriesid = contract.seriesid

		dat = pandas.read_csv(path, header=None, names=['date', 'time', 'open_', 'high', 'low', 'close_', 'volume'])
		dat['seriesid'] = seriesid
		dat['contractid'] = contractid
		dat['dt'] = pandas.to_datetime(dat.date + ' ' + dat.time)

		dat = dat[['contractid', 'seriesid', 'dt',  'open_', 'high', 'low', 'close_', 'volume']]
		dat_dict = dat.to_dict(orient='records')
		import pdb; pdb.set_trace()
		cls.__table__.insert().execute(dat_dict)


	def load(self, seriesid, ticker=None, year_begin=2000):
		from db.models.trading import Series, Contract

		series = Series.filter(Series.id==seriesid).first()
		contracts = Contract.filter(Contract.seriesid==series.id).all()

		intraday_files = os.listdir(INTRADAY_PATH)

		for contract in contracts:
			expiry_yr = str(contract.expiry_dt.year)
			cusip = contract.cusip.replace(expiry_yr, expiry_yr[-2:])

			if ticker:
				cusip = cusip.replace(series.ticker, ticker)

			try:
				dat = pandas.read_csv(os.path.join(INTRADAY_PATH, '{0}.txt'.format(cusip)), header=None,
									  names=['date', 'time', 'open_', 'high', 'low', 'close_', 'volume'])
			except IOError:
				continue

			dat['dt'] = pandas.to_datetime(dat.date + ' ' + dat.time)
			dat['contractid'] = contract.id
			dat['seriesid'] = series.id

			dat = dat[['contractid', 'seriesid', 'dt',  'open_', 'high', 'low', 'close_', 'volume']]
			dat_dict = dat.to_dict(orient='records')

			print "Inserting {0}".format(cusip)
			self.__class__.__table__.insert().execute(dat_dict)

		print 'Done'

	def pull_times_local(self, seriesid, local_time):
		from db.models.trading import Series, Contract
		series = Series.filter(Series.id == seriesid).first()
		hist_daily = pandas.DataFrame(session.query(Daily.dt, Daily.contractid, Daily.close_.label('settle'), Daily.chg, Daily.atr10).join(Contract).filter(and_(Daily.contract_num==1, Contract.seriesid==series.id)).all())
		local_times = hist_daily.dt.map(lambda x, t=local_time, tz=series.tz: convert_EST(datetime.datetime.combine(x, t), tz))
		local_times = [local_times.iloc[i].to_pydatetime() for i in xrange(len(local_times))]

		hist_intraday = pandas.DataFrame()
		i = 0
		while True:
			top_range =min(i+500, len(local_times))

			local_times_chunk = local_times[i:top_range]
			hist_intraday = hist_intraday.append(Intraday.filter_df(and_(Intraday.seriesid==series.id, Intraday.dt.in_(local_times_chunk))))

			if top_range == len(local_times):
				break

			i += 500

		hist_intraday.dt = hist_intraday.dt.map(lambda a: a.date())
		hist_daily.dt = hist_daily.dt.map(lambda a: a.date())
		hist_data = hist_intraday.merge(hist_daily, on=['dt', 'contractid'])

		return hist_data[['dt', 'contractid', 'open_', 'high', 'low', 'close_', 'settle', 'chg', 'atr10']]

	@classmethod
	def pull_minute_bars_local(cls, seriesid, local_time, offset=30, contract_num=1):
		from db.models.trading import Series, Contract

		dt_offset = Table('DtOffset', Base.metadata, Column('id', Integer, primary_key=True),
						  Column('contractid', Integer, nullable=False),
						  Column('futdate', DateTime, nullable=False),
						  Column('dt_start', DateTime, nullable=False),
						  Column('dt_end', DateTime, nullable=False),
						  prefixes=['TEMPORARY'])
		dt_offset.create()

		cmd = dt_offset.delete()
		cmd.execute()

		series = Series.filter(Series.id==seriesid).first()
		hist_daily = pandas.DataFrame(session.query(Daily.dt, Daily.contractid, Daily.close_.label('settle'), Daily.chg, Daily.atr10).join(Contract).filter(and_(Daily.contract_num==contract_num, Contract.seriesid==series.id)).all())


		local_times = pandas.to_datetime(hist_daily.dt.map(lambda x, t=local_time, tz=series.tz: convert_EST(datetime.datetime.combine(x, t), tz)))
		local_times_nontz = numpy.array([None] * len(local_times))

		for i in xrange(len(local_times)):
			local_times_nontz[i] = local_times.iloc[i].replace(tzinfo=None)

		df_insert = pandas.DataFrame({'dt_start':local_times_nontz, 'dt_end': local_times_nontz + datetime.timedelta(minutes=offset), 'futdate': hist_daily.dt.values, 'contractid':hist_daily.contractid.values})
		df_insert.to_sql('DtOffset', engine, if_exists='append', index=False) #append to temp table

		query = session.query(Intraday, dt_offset.c.dt_start, dt_offset.c.dt_end).join((dt_offset, and_(Intraday.dt.between(dt_offset.c.dt_start, dt_offset.c.dt_end), Intraday.contractid==dt_offset.c.contractid)))
		# import pdb; pdb.set_trace()
		df = pandas.read_sql(query.statement, query.session.bind)

		dt_offset.drop(engine)
		Base.metadata.remove(dt_offset)
		return df

	@classmethod
	def pull_minute_bars(cls, seriesid, time, offset=0, contract_num=1):
		from db.models.trading import Series, Contract

		dt_offset = Table('DtOffset', Base.metadata, Column('id', Integer, primary_key=True),
						  Column('contractid', Integer, nullable=False),
						  Column('futdate', DateTime, nullable=False),
						  Column('dt_start', DateTime, nullable=False),
						  Column('dt_end', DateTime, nullable=False),
						  prefixes=['TEMPORARY'])
		dt_offset.create()

		cmd = dt_offset.delete()
		cmd.execute()

		series = Series.filter(Series.id==seriesid).first()
		hist_daily = pandas.DataFrame(session.query(Daily.dt, Daily.contractid, Daily.close_.label('settle'), Daily.chg, Daily.atr10).join(Contract).filter(and_(Daily.contract_num==contract_num, Contract.seriesid==series.id)).all())


		local_times = pandas.to_datetime(hist_daily.dt.map(lambda x, t=time, tz=series.tz: datetime.datetime.combine(x, t)))
		local_times_nontz = numpy.array([None] * len(local_times))

		for i in xrange(len(local_times)):
			local_times_nontz[i] = local_times.iloc[i].replace(tzinfo=None)

		df_insert = pandas.DataFrame({'dt_start':local_times_nontz, 'dt_end': local_times_nontz + datetime.timedelta(minutes=offset), 'futdate': hist_daily.dt.values, 'contractid':hist_daily.contractid.values})
		df_insert.to_sql('DtOffset', engine, if_exists='append', index=False) #append to temp table

		query = session.query(Intraday, dt_offset.c.dt_start, dt_offset.c.dt_end).join((dt_offset, and_(Intraday.dt.between(dt_offset.c.dt_start, dt_offset.c.dt_end), Intraday.contractid==dt_offset.c.contractid)))
		# import pdb; pdb.set_trace()
		df = pandas.read_sql(query.statement, query.session.bind)
		df['dt'] = df.dt.map(lambda a: a.date())
		hist_daily['dt'] = hist_daily.dt.map(lambda a: a.date())
		df = df.merge(hist_daily, on=['dt', 'contractid'])

		dt_offset.drop(engine)
		Base.metadata.remove(dt_offset)
		return df.sort('dt')

	@classmethod
	def pull_ohlc_local(cls, seriesid, local_time, offset=30, times=False):
		from db.models.trading import Series, Contract

		dt_offset = Table('DtOffset', Base.metadata, Column('id', Integer, primary_key=True),
						  Column('contractid', Integer, nullable=False),
						  Column('futdate', DateTime, nullable=False),
						  Column('dt_start', DateTime, nullable=False),
						  Column('dt_end', DateTime, nullable=False),
						  prefixes=['TEMPORARY'])
		dt_offset.create()

		cmd = dt_offset.delete()
		cmd.execute()

		series = Series.filter(Series.id==seriesid).first()
		hist_daily = pandas.DataFrame(session.query(Daily.dt, Daily.contractid, Daily.close_.label('settle'), Daily.volume, Daily.chg, Daily.atr10).join(Contract).filter(and_(Daily.contract_num==1, Contract.seriesid==series.id)).all())


		local_times = pandas.to_datetime(hist_daily.dt.map(lambda x, t=local_time, tz=series.tz: convert_EST(datetime.datetime.combine(x, t), tz)))
		local_times_nontz = numpy.array([None] * len(local_times))

		for i in xrange(len(local_times)):
			local_times_nontz[i] = local_times.iloc[i].replace(tzinfo=None)

		df_insert = pandas.DataFrame({'dt_start':local_times_nontz, 'dt_end': local_times_nontz + datetime.timedelta(minutes=offset), 'futdate': hist_daily.dt.values, 'contractid':hist_daily.contractid.values})
		df_insert.to_sql('DtOffset', engine, if_exists='append', index=False) #append to temp table

		query = session.query(Intraday, dt_offset.c.dt_start, dt_offset.c.dt_end).join((dt_offset, and_(Intraday.dt.between(dt_offset.c.dt_start, dt_offset.c.dt_end), Intraday.contractid==dt_offset.c.contractid)))
		# import pdb; pdb.set_trace()
		df = pandas.read_sql(query.statement, query.session.bind)

		df['futdate'] = df.dt_start.map(lambda a, f=fut_date: f(a))
		df.index = pandas.to_datetime(df.dt)

		df = df.dropna()
		df_ohlc = pandas.DataFrame()

		gb = df.groupby('futdate')
		df_ohlc['open'] = gb.first().open_
		df_ohlc['close'] = gb.last().close_
		df_ohlc['start_time'] = gb.first().dt_start
		df_ohlc['end_time'] = gb.first().dt_end
		df_ohlc['ivolume'] = gb.sum().volume

		highs = gb.max().high
		lows = gb.min().low

		df_ohlc['high'] = highs
		df_ohlc['low'] = lows

		df_ohlc['dt'] = pandas.to_datetime(df_ohlc.index)

		if times:
			highs = pandas.DataFrame(highs)
			lows = pandas.DataFrame(lows)
			highs['date'] = pandas.to_datetime(highs.index)
			lows['date'] = pandas.to_datetime(lows.index)

			df['dt'] = pandas.to_datetime(df.index)
			df['date'] = pandas.to_datetime(df.dt_start.map(lambda a, f=fut_date: f(a)))
			# import pdb; pdb.set_trace()
			high_times = pandas.DataFrame(highs.merge(df, on=['date', 'high'], how='left').dropna().groupby('date').dt.min())
			high_times.columns = ['high_time']
			high_times['dt'] = pandas.to_datetime(high_times.index)
			low_times = pandas.DataFrame(lows.merge(df, on=['date', 'low'], how='left').dropna().groupby('date').dt.min())
			low_times.columns = ['low_time']
			low_times['dt'] = pandas.to_datetime(low_times.index)

			df_ohlc = df_ohlc.merge(high_times, on='dt', how='left').merge(low_times, on='dt', how='left')

		df_final = df_ohlc.merge(hist_daily[['dt', 'contractid', 'settle', 'chg', 'atr10', 'volume']], on='dt', how='left')

		dt_offset.drop(engine)
		Base.metadata.remove(dt_offset)
		return df_final.dropna()

'''
class Intraday(Base):
	__tablename__ = 'Intraday2'

	seriesid = Column(Integer, ForeignKey('Series.id'))
	contractid = Column(Integer, ForeignKey('Contract.id'), primary_key=True)
	dt = Column(DateTime, nullable=False, primary_key=True)
	open_ = Column(Float, nullable=True)
	high = Column(Float, nullable=True)
	low = Column(Float, nullable=True)
	close_ = Column(Float, nullable=True)
	volume = Column(Integer, nullable=True)

	__table_args__ = (UniqueConstraint('dt', 'contractid'),)

	@classmethod
	def load_file(cls, contractid, path):
		from db.models.trading import Series, Contract
		contract = Contract.filter(Contract.id == contractid).first()
		seriesid = contract.seriesid

		dat = pandas.read_csv(path, header=None, names=['date', 'time', 'open_', 'high', 'low', 'close_', 'volume'])
		dat['seriesid'] = seriesid
		dat['contractid'] = contractid
		dat['dt'] = pandas.to_datetime(dat.date + ' ' + dat.time)

		dat = dat[['contractid', 'seriesid', 'dt',  'open_', 'high', 'low', 'close_', 'volume']]
		dat_dict = dat.to_dict(orient='records')
		import pdb; pdb.set_trace()
		cls.__table__.insert().execute(dat_dict)


	def load(self, seriesid, ticker=None, year_begin=2000):
		from db.models.trading import Series, Contract

		series = Series.filter(Series.id==seriesid).first()
		contracts = Contract.filter(Contract.seriesid==series.id).all()

		intraday_files = os.listdir(INTRADAY_PATH)

		for contract in contracts:
			expiry_yr = str(contract.expiry_dt.year)
			cusip = contract.cusip.replace(expiry_yr, expiry_yr[-2:])

			if ticker:
				cusip = cusip.replace(series.ticker, ticker)

			try:
				dat = pandas.read_csv(os.path.join(INTRADAY_PATH, '{0}.txt'.format(cusip)), header=None,
									  names=['date', 'time', 'open_', 'high', 'low', 'close_', 'volume'])
			except IOError:
				continue

			dat['dt'] = pandas.to_datetime(dat.date + ' ' + dat.time)
			dat['contractid'] = contract.id
			dat['seriesid'] = series.id

			dat = dat[['contractid', 'seriesid', 'dt',  'open_', 'high', 'low', 'close_', 'volume']]
			dat_dict = dat.to_dict(orient='records')

			print "Inserting {0}".format(cusip)
			self.__class__.__table__.insert().execute(dat_dict)

		print 'Done'

	@classmethod
	def pull_times_local(cls, seriesid, local_time):
		from db.models.trading import Series, Contract
		series = Series.filter(Series.id == seriesid).first()
		hist_daily = pandas.DataFrame(session.query(Daily.dt, Daily.contractid, Daily.close_.label('settle'), Daily.chg, Daily.atr10).join(Contract).filter(and_(Daily.contract_num==1, Contract.seriesid==series.id)).all())
		local_times = hist_daily.dt.map(lambda x, t=local_time, tz=series.tz: convert_EST(datetime.datetime.combine(x, t), tz))
		local_times = [local_times.iloc[i].to_pydatetime() for i in xrange(len(local_times))]

		hist_intraday = pandas.DataFrame()
		i = 0
		while True:
			top_range =min(i+500, len(local_times))

			local_times_chunk = local_times[i:top_range]
			hist_intraday = hist_intraday.append(Intraday.filter_df(and_(Intraday.seriesid==series.id, Intraday.dt.in_(local_times_chunk))))

			if top_range == len(local_times):
				break

			i += 500

		hist_intraday.dt = hist_intraday.dt.map(lambda a: a.date())
		hist_daily.dt = hist_daily.dt.map(lambda a: a.date())
		hist_data = hist_intraday.merge(hist_daily, on=['dt', 'contractid'])

		return hist_data[['dt', 'contractid', 'open_', 'high', 'low', 'close_', 'settle', 'chg', 'atr10']]

	@classmethod
	def pull_minute_bars_local(cls, seriesid, local_time, offset=30, contract_num=1):
		from db.models.trading import Series, Contract

		dt_offset = Table('DtOffset', Base.metadata, Column('id', Integer, primary_key=True),
						  Column('contractid', Integer, nullable=False),
						  Column('futdate', DateTime, nullable=False),
						  Column('dt_start', DateTime, nullable=False),
						  Column('dt_end', DateTime, nullable=False),
						  prefixes=['TEMPORARY'])
		dt_offset.create()

		cmd = dt_offset.delete()
		cmd.execute()

		series = Series.filter(Series.id==seriesid).first()
		hist_daily = pandas.DataFrame(session.query(Daily.dt, Daily.contractid, Daily.close_.label('settle'), Daily.chg, Daily.atr10).join(Contract).filter(and_(Daily.contract_num==contract_num, Contract.seriesid==series.id)).all())


		local_times = pandas.to_datetime(hist_daily.dt.map(lambda x, t=local_time, tz=series.tz: convert_EST(datetime.datetime.combine(x, t), tz)))
		local_times_nontz = numpy.array([None] * len(local_times))

		for i in xrange(len(local_times)):
			local_times_nontz[i] = local_times.iloc[i].replace(tzinfo=None)

		df_insert = pandas.DataFrame({'dt_start':local_times_nontz, 'dt_end': local_times_nontz + datetime.timedelta(minutes=offset), 'futdate': hist_daily.dt.values, 'contractid':hist_daily.contractid.values})
		df_insert.to_sql('DtOffset', engine, if_exists='append', index=False) #append to temp table

		query = session.query(Intraday, dt_offset.c.dt_start, dt_offset.c.dt_end).join((dt_offset, and_(Intraday.dt.between(dt_offset.c.dt_start, dt_offset.c.dt_end), Intraday.contractid==dt_offset.c.contractid)))
		# import pdb; pdb.set_trace()
		df = pandas.read_sql(query.statement, query.session.bind)

		dt_offset.drop(engine)
		Base.metadata.remove(dt_offset)
		return df

	@classmethod
	def pull_minute_bars(cls, seriesid, time, offset=0, contract_num=1):
		from db.models.trading import Series, Contract

		dt_offset = Table('DtOffset', Base.metadata, Column('id', Integer, primary_key=True),
						  Column('contractid', Integer, nullable=False),
						  Column('futdate', DateTime, nullable=False),
						  Column('dt_start', DateTime, nullable=False),
						  Column('dt_end', DateTime, nullable=False),
						  prefixes=['TEMPORARY'])
		dt_offset.create()

		cmd = dt_offset.delete()
		cmd.execute()

		series = Series.filter(Series.id==seriesid).first()
		hist_daily = pandas.DataFrame(session.query(Daily.dt, Daily.contractid, Daily.close_.label('settle'), Daily.chg, Daily.atr10).join(Contract).filter(and_(Daily.contract_num==contract_num, Contract.seriesid==series.id)).all())


		local_times = pandas.to_datetime(hist_daily.dt.map(lambda x, t=time, tz=series.tz: datetime.datetime.combine(x, t)))
		local_times_nontz = numpy.array([None] * len(local_times))

		for i in xrange(len(local_times)):
			local_times_nontz[i] = local_times.iloc[i].replace(tzinfo=None)

		df_insert = pandas.DataFrame({'dt_start':local_times_nontz, 'dt_end': local_times_nontz + datetime.timedelta(minutes=offset), 'futdate': hist_daily.dt.values, 'contractid':hist_daily.contractid.values})
		df_insert.to_sql('DtOffset', engine, if_exists='append', index=False) #append to temp table

		query = session.query(Intraday, dt_offset.c.dt_start, dt_offset.c.dt_end).join((dt_offset, and_(Intraday.dt.between(dt_offset.c.dt_start, dt_offset.c.dt_end), Intraday.contractid==dt_offset.c.contractid)))
		# import pdb; pdb.set_trace()
		df = pandas.read_sql(query.statement, query.session.bind)
		df['dt'] = df.dt.map(lambda a: a.date())
		hist_daily['dt'] = hist_daily.dt.map(lambda a: a.date())
		df = df.merge(hist_daily, on=['dt', 'contractid'])

		dt_offset.drop(engine)
		Base.metadata.remove(dt_offset)
		return df.sort('dt')

	@classmethod
	def pull_ohlc_local(cls, seriesid, local_time, offset=30, contract_num=1, times=False):
		from db.models.trading import Series, Contract

		dt_offset = Table('DtOffset', Base.metadata, Column('id', Integer, primary_key=True),
						  Column('contractid', Integer, nullable=False),
						  Column('futdate', DateTime, nullable=False),
						  Column('dt_start', DateTime, nullable=False),
						  Column('dt_end', DateTime, nullable=False),
						  prefixes=['TEMPORARY'])
		dt_offset.create()

		cmd = dt_offset.delete()
		cmd.execute()

		series = Series.filter(Series.id==seriesid).first()
		hist_daily = pandas.DataFrame(session.query(Daily.dt, Daily.contractid, Daily.close_.label('settle'), Daily.open_, Daily.volume, Daily.chg, Daily.atr10).join(Contract).filter(and_(Daily.contract_num==contract_num, Contract.seriesid==series.id)).all())


		local_times = pandas.to_datetime(hist_daily.dt.map(lambda x, t=local_time, tz=series.tz: convert_EST(datetime.datetime.combine(x, t), tz)))
		local_times_nontz = numpy.array([None] * len(local_times))

		for i in xrange(len(local_times)):
			local_times_nontz[i] = local_times.iloc[i].replace(tzinfo=None)

		df_insert = pandas.DataFrame({'dt_start':local_times_nontz, 'dt_end': local_times_nontz + datetime.timedelta(minutes=offset), 'futdate': hist_daily.dt.values, 'contractid':hist_daily.contractid.values})
		df_insert.to_sql('DtOffset', engine, if_exists='append', index=False) #append to temp table

		query = session.query(Intraday, dt_offset.c.dt_start, dt_offset.c.dt_end).join((dt_offset, and_(Intraday.dt.between(dt_offset.c.dt_start, dt_offset.c.dt_end), Intraday.contractid==dt_offset.c.contractid)))
		# import pdb; pdb.set_trace()
		df = pandas.read_sql(query.statement, query.session.bind)

		df['futdate'] = df.dt_start.map(lambda a, f=fut_date: f(a))
		df.index = pandas.to_datetime(df.dt)

		df = df.dropna()
		df_ohlc = pandas.DataFrame()

		gb = df.groupby('futdate')
		df_ohlc['open'] = gb.first().open_
		df_ohlc['close'] = gb.last().close_
		df_ohlc['start_time'] = gb.first().dt_start
		df_ohlc['end_time'] = gb.first().dt_end
		df_ohlc['ivolume'] = gb.sum().volume

		highs = gb.max().high
		lows = gb.min().low

		df_ohlc['high'] = highs
		df_ohlc['low'] = lows

		df_ohlc['dt'] = pandas.to_datetime(df_ohlc.index)

		if times:
			highs = pandas.DataFrame(highs)
			lows = pandas.DataFrame(lows)
			highs['date'] = pandas.to_datetime(highs.index)
			lows['date'] = pandas.to_datetime(lows.index)

			df['dt'] = pandas.to_datetime(df.index)
			df['date'] = pandas.to_datetime(df.dt_start.map(lambda a, f=fut_date: f(a)))
			# import pdb; pdb.set_trace()
			high_times = pandas.DataFrame(highs.merge(df, on=['date', 'high'], how='left').dropna().groupby('date').dt.min())
			high_times.columns = ['high_time']
			high_times['dt'] = pandas.to_datetime(high_times.index)
			low_times = pandas.DataFrame(lows.merge(df, on=['date', 'low'], how='left').dropna().groupby('date').dt.min())
			low_times.columns = ['low_time']
			low_times['dt'] = pandas.to_datetime(low_times.index)

			df_ohlc = df_ohlc.merge(high_times, on='dt', how='left').merge(low_times, on='dt', how='left')

		df_final = df_ohlc.merge(hist_daily[['dt', 'contractid', 'open_', 'settle', 'chg', 'atr10', 'volume']], on='dt', how='left')

		dt_offset.drop(engine)
		Base.metadata.remove(dt_offset)
		return df_final.dropna()

class Daily(Base):
	__tablename__ = 'Daily'
	
	id = Column(Integer, primary_key=True)
	contractid = Column(Integer, ForeignKey('Contract.id'))
	dt = Column(DateTime, nullable=False)
	contract_num = Column(Integer, nullable=True)
	open_ = Column(Float, nullable=True)
	high = Column(Float, nullable=True)
	low = Column(Float, nullable=True)
	close_ = Column(Float, nullable=True)
	volume = Column(Integer, nullable=True)
	chg = Column(Float, nullable=True)
	atr10 = Column(Float, nullable=True)
	
	__table_args__ = (UniqueConstraint('dt', 'contractid'),)

	def load(self, seriesid, year_begin=2000):
		series = Series.filter(Series.id==seriesid).first()
		year = year_begin
		history_dat = pandas.DataFrame()

		if series.type == 'IND' or series.type == 'CASH':
			quandl_data = Quandl.get(series.quandl_code, authtoken=QUANDL_API_KEY)

			quandl_data['dt'] = pandas.to_datetime(quandl_data.index)
			quandl_data = quandl_data[quandl_data.dt > datetime.date(year, 1, 1)]

			if 'Value' in quandl_data and 'Close' not in quandl_data:
				quandl_data = quandl_data.rename(columns={'Value': 'Close'})

			#need to rename volume column
			volume_column = quandl_data.columns[['volu' in col.lower() for col in quandl_data.columns]][0]
			quandl_data = quandl_data.rename(columns={volume_column:'Volume'})

			for column in ['High', 'Low', 'Open', 'Volume']:
				if column not in quandl_data:
					quandl_data[column] = None

			#load into contracts
			contract = Contract(seriesid=series.id,
								cusip=series.ticker)

			contract.save()

			#pull daily data
			quandl_data['dt'] = quandl_data.index
			quandl_data['contractid'] = contract.id
			quandl_data['contract_num'] = None
			quandl_data['open_'] = quandl_data.Open
			quandl_data['high'] = quandl_data.High
			quandl_data['low'] = quandl_data.Low
			quandl_data['close_'] = quandl_data.Close
			quandl_data['volume'] = quandl_data.Volume
			quandl_data['chg'] = quandl_data.close_.diff(1)

			history_dat = history_dat.append(quandl_data)

		elif series.type == 'FUT':
			#insert data into Daily
			while year:
				for delivery_code in series.delivery:
					#quandl
					quandl_code = '{0}{1}{2}'.format(series.quandl_code, delivery_code, year)
					cusip = '{0}{1}{2}'.format(series.ticker, delivery_code, year)

					print 'Loading {0}'.format(cusip)

					#ibcontract
					ibcontract = IBcontract()
					ibcontract.secType = str(series.type)
					ibcontract.exchange = str(series.exchange)
					ibcontract.symbol = str(series.symbol)

					if series.type == 'FUT':
						ibcontract.expiry = str(year) + "%02d" % FUT_CONTRACT_CODES[delivery_code]
						# ibcontract.localSymbol = str('{0}{1}{2}'.format(self.ticker, delivery_code, str(year)[-1]))
						ibcontract.localSymbol = str('{0} {1} {2}'.format(self.ticker, datetime.date(1900, FUT_CONTRACT_CODES[delivery_code], 1).strftime('%b'), str(year)[-2:]).upper())
					try:
						quandl_data = Quandl.get(quandl_code, authtoken=QUANDL_API_KEY)

						#need to rename volume column
						volume_column = quandl_data.columns[['volu' in col.lower() for col in quandl_data.columns]][0]
						quandl_data = quandl_data.rename(columns={volume_column:'Volume'})

						quandl_data = quandl_data[quandl_data.Volume > 0]

						#quandl expiry
						expiry_dt = quandl_data.iloc[-1].name
						roll_dt = quandl_data.iloc[-5].name


						#load into contracts
						contract = Contract(seriesid=self.id,
											cusip=cusip,
											expiry_month=ibcontract.expiry,
											roll=roll_dt,
											expiry_dt=expiry_dt)


						#check ibcontract expiry
						if (today - expiry_dt).days <= 5:
							import pdb; pdb.set_trace()
							ibcontract.expiry = ""
							contract.expiry_dt = None
							ibcontract.localSymbol = contract.local_cusip
							details = client.get_contract_details(ibcontract)
							expiry_dt = datetime.datetime.strptime(details['expiry'], '%Y%m%d')
							roll_dt = expiry_dt - datetime.timedelta(days=7)
							contract.expiry_dt = expiry_dt
							contract.roll = roll_dt


						contract.save()

						#pull daily data
						quandl_data['dt'] = quandl_data.index
						quandl_data['contractid'] = contract.id
						quandl_data['contract_num'] = None
						quandl_data['open_'] = quandl_data.Open
						quandl_data['high'] = quandl_data.High
						quandl_data['low'] = quandl_data.Low
						quandl_data['close_'] = quandl_data.Settle
						quandl_data['volume'] = quandl_data.Volume
						quandl_data['chg'] = quandl_data.close_.diff(1)
						quandl_data['roll'] = roll_dt


						history_dat = history_dat.append(quandl_data)

					except (Quandl.Quandl.DatasetNotFound, IndexError):
						year = False
						break

				#increment year
				if year:
					year += 1

			#insert contract nums into daily
			for dt in history_dat.dt.unique():
				history_dat.ix[(history_dat.dt == dt) & (history_dat.dt < history_dat.roll), 'contract_num'] = numpy.array(range(len(history_dat[(history_dat.dt == dt) & (history_dat.dt < history_dat.roll)]))) + 1
		else:
			pass




