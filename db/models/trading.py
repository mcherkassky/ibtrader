from db.sqlite import *
from db.models.data import *
import datetime
import numpy
import traceback
import talib
from time import sleep
from sqlalchemy.sql.expression import bindparam
from sqlalchemy import desc
from sqlalchemy.exc import StatementError
from utils import convert_local,fut_date, clone_table
import urllib2
from swigibpy import ComboLeg, ComboLegList
from utils.config import EMAIL_CRITICAL, EMAIL_TEXT


#persist local timezone
local_tz = get_localzone()

def now_offset():
	return datetime.datetime.now() + datetime.timedelta(seconds=60)

class Series(Base):
	__tablename__ = 'Series'

	id = Column(Integer, primary_key=True)
	symbol = Column(String(5), nullable=False)
	ticker = Column(String(5), nullable=False)
	name = Column(String(100), nullable=False)
	type = Column(String(5), nullable=False)
	exchange = Column(String(100), nullable=False)
	quandl_code = Column(String(15), nullable=False)

	tick = Column(Float, nullable=True)
	cupp = Column(Float, nullable=True)
	delivery = Column(String(30), nullable=True)

	liq_start = Column(Time, nullable=True)
	liq_end = Column(Time, nullable=True)
	open_time = Column(Time, nullable=True)
	close_time = Column(Time, nullable=True)

	active = Column(Boolean, nullable=True)
	local_code = Column(Integer, nullable=True)
	currency = Column(String(5), nullable=True)
	tz = Column(String(50), nullable=True)
	roll_days = Column(Integer, nullable=True)

	#get front contract
	@property
	def front_contract(self):
		return self.contract(contract_num=1)

	def nearest_tick(self, px):
		return round(px/self.tick) * self.tick

	def contract(self, contract_num=1, strategy=None):
		try:
			if strategy:
				# if strategy.live:
				# 	today = datetime.datetime.today()
				# else:
				today = strategy.today
			else:
				today = datetime.datetime.today()
		except:
			today = datetime.datetime.today()

		front_contract = Contract.filter(Contract.roll > today, Contract.seriesid == self.id).order_by(Contract.expiry_dt).first()

		if contract_num == 1:
			return front_contract
		elif contract_num > 1:
			future_contracts = Contract.filter(Contract.roll >= front_contract.roll, Contract.seriesid == front_contract.seriesid).order_by(Contract.expiry_dt).all()
			return future_contracts[contract_num - 1]
		elif contract_num < 0:
			past_contracts = Contract.filter(Contract.roll < front_contract.roll, Contract.seriesid == front_contract.seriesid).order_by(Contract.expiry_dt).all()
			return past_contracts[contract_num]
		else:
			raise Exception("Invalid Contract Number")

	def data(self, start_date=datetime.datetime(2000,1,1), end_date=None, contract_num=1, ret_obj=False, filter_forward=True, roll_ohlc=False, offset=None, strategy=None):
		if not end_date:
			if strategy:
				end_date = strategy.today
			else:
				end_date = fut_date(datetime.datetime.now())

			if not filter_forward:
				end_date = end_date + datetime.timedelta(days=1)

		if offset:
			start_date = end_date - datetime.timedelta(days=offset)


		rows = session.query(Daily).join(Contract).join(self.__class__).filter(and_(self.__class__.id == self.id,
																					Daily.contract_num == contract_num,
																					Daily.dt >= start_date,
																					Daily.dt <= end_date)).all()
		if ret_obj:
			return rows
		else:
			rows_dict = [r.__dict__ for r in rows]

			df = pandas.DataFrame(rows_dict).drop('_sa_instance_state', 1)
			df['rolled'] = df.chg.cumsum() - df.iloc[0].chg + df.iloc[0].close_
			df['settle_yest'] = df.close_ - df.chg

			if roll_ohlc:
				df['roll_correction'] = -(df.close_ - df.rolled)
				df['rolled_high'] = (df.high + df.roll_correction).fillna(method='ffill')
				df['rolled_low'] = df.low + df.roll_correction.fillna(method='ffill')
				df['rolled_open_'] = df.open_ + df.roll_correction.fillna(method='ffill')
				df['rolled_close_'] = df.close_ + df.roll_correction.fillna(method='ffill')

				return df[['id', 'dt', 'contractid', 'contract_num', 'open_', 'high', 'low', 'close_', 'settle_yest', 'atr10', 'rolled', 'chg', 'volume', 'rolled_open_', 'rolled_high', 'rolled_low', 'rolled_close_', 'roll_correction']]

			return df[['id', 'dt', 'contractid', 'contract_num', 'open_', 'high', 'low', 'close_', 'settle_yest', 'atr10', 'rolled', 'chg', 'volume']]

	def update(self, client):
		#update contracts
		self.update_contracts(client)

		#update daily
		self.update_daily(client)

		#update atr
		self.update_atr(client)

		#update intraday
		self.update_intraday(client)

	def update_intraday(self, client):
		if self.type <> 'FUT':
			return None

		today = datetime.datetime.now()

		if (fut_date(today) - today.date()).days > 0:
			contracts = Contract.filter(and_(Contract.seriesid==self.id, Contract.expiry_dt>=today.replace(hour=0, minute=0, second=0, microsecond=0))).order_by(Contract.expiry_dt).all()

			for contract in contracts[:min(3, len(contracts))]:
				print 'Updating {0}'.format(contract.cusip)
				try:
					endDateTime = convert_local(today.replace(hour=18, minute=0, second=0, microsecond=0), 'US/Eastern').replace(tzinfo=None)
					intraday = contract.hist_data(client, durationStr="86400 S", barSizeSetting="1 min", endDateTime=endDateTime)

				except Exception as e:
					print(e)
					continue

				#persist flat files
				outdir = './flat/{0}/{1}'.format(contract.seriesid, contract.id)
				if not os.path.exists(outdir):
					os.makedirs(outdir)
				intraday.to_csv('{0}/{1}.csv'.format(outdir, endDateTime.strftime('%Y%m%d')))

				#format
				intraday['dt'] = pandas.to_datetime(intraday.index)
				intraday['seriesid'] = self.id
				intraday['contractid'] = contract.id

				#change tz
				intraday['dt'] = intraday.dt.map(lambda a: convert_EST(a, local_tz.zone))

				#delete old data
				Intraday.filter(and_(Intraday.dt >= intraday.dt.min(), Intraday.dt <=intraday.dt.max(),  Intraday.contractid==contract.id)).delete()
				session.commit()

				intraday = intraday.rename(columns={'open':'open_', 'close':'close_'})

				#upload
				intraday_dict = intraday.to_dict(orient='records')

				Intraday.__table__.insert().execute(intraday_dict)

	def update_daily(self, client):
		today = datetime.datetime.now()
		contracts = Contract.filter(and_(Contract.seriesid==self.id, Contract.expiry_dt>=today.replace(hour=0, minute=0, second=0, microsecond=0)))

		max_date = session.query(func.max(Daily.dt)).filter(Daily.contractid == self.front_contract.id).first()[0]
		data_upload = pandas.DataFrame()

		contract_counter = 1
		for contract in contracts.order_by(Contract.id):
			if contract_counter > 10:
				break
			try:
				daily = contract.hist_data(client, durationStr="20 D")
				# daily2 = contract.hist_data(client, durationStr="50 D", endDateTime=daily.iloc[0].name.to_datetime() - datetime.timedelta(days=1))
				# daily3 = contract.hist_data(client, durationStr="50 D", endDateTime=daily2.iloc[0].name.to_datetime() - datetime.timedelta(days=1))
				# daily = daily3.append(daily2).append(daily)

			except Exception as e:
				print(e)
				contract_counter += 1

				#email error
				if contract_counter <= 3:
					email = Email("CRITICAL: {0} Data Update Failed - Restart TWS".format(contract.cusip), "")
					email.send(EMAIL_CRITICAL)
				continue

			daily['dt'] = pandas.to_datetime(daily.index)
			daily['chg'] = daily.close.diff()
			daily['contractid'] = contract.id
			daily['roll'] = contract.roll
			daily['contract_num'] = None
			daily['atr10'] = None

			if daily.dt.max().to_datetime().date() < fut_date(datetime.datetime.now()):
				#details = client.get_contract_details(contract.contract)
				#tomorrow_dt = datetime.datetime.strptime(details['liquidHours'].split(';')[1].split(':')[0], '%Y%m%d')
				#tomorrow_dt = datetime.datetime.combine(tomorrow_dt, datetime.datetime.min.time())
				last_day = daily.iloc[-1]['dt'].to_datetime()
				tomorrow_dt = last_day + datetime.timedelta(days=1) if last_day.weekday() <4 else last_day + datetime.timedelta(days=3)
				next_day = daily.iloc[-1]
				next_day['dt'] = tomorrow_dt
				next_day.chg = 0
				daily = daily.append(next_day)


			#get new data
			# daily_new = daily[(daily.dt <= today.date()) & ~(numpy.isnan(daily.chg))]
			daily_new = daily[~(numpy.isnan(daily.chg))]

			#delete possibly stale data
			try:
				Daily.filter(and_(Daily.dt >= daily_new.dt.min(), Daily.contractid==contract.id)).delete()
				session.commit()
			except StatementError:
				print 'Non-critical Data Error - continuing to next contract'
				continue

			data_upload = data_upload.append(daily_new)

			contract_counter += 1

		#data still in db
		try:
			query = session.query(Daily, Contract.roll).join(Contract).filter(Contract.seriesid==self.id, Daily.dt >=data_upload.dt.min())
		except:
			return None
		data_remain = pandas.read_sql(query.statement, query.session.bind).drop('id', 1)
		data_remain['indic'] = 1

		#add contract_nums
		data_upload['indic'] = 0
		data_upload = data_upload.rename(columns={'open':'open_', 'close':'close_'})
		data_upload = data_upload[data_remain.columns]
		data_upload = data_upload.append(data_remain).sort_values('roll')

		if self.type == 'FUT':
			for dt in data_upload.dt.unique():
				data_upload.ix[(data_upload.dt == dt) & (data_upload.dt < data_upload.roll), 'contract_num'] = numpy.array(range(len(data_upload[(data_upload.dt == dt) & (data_upload.dt < data_upload.roll)]))) + 1

		elif self.type == 'IND':
			data_upload['contract_num'] = 1
		else:
			pass

		if len(data_upload):
			dat = data_upload[data_upload.indic == 0][['contractid', 'dt', 'contract_num', 'open_', 'high', 'low', 'close_', 'volume', 'chg', 'atr10']]
			dat_dict = dat.to_dict(orient='records')


			Daily.__table__.insert().execute(dat_dict)

	def insert_history(self, client, start=2000):
		today = datetime.datetime.now()
		year = start

		history_dat = pandas.DataFrame()

		if self.type == 'IND' or self.type == 'CASH':

			quandl_data = Quandl.get(self.quandl_code, authtoken=QUANDL_API_KEY)

			quandl_data['dt'] = pandas.to_datetime(quandl_data.index)
			quandl_data = quandl_data[quandl_data.dt > datetime.date(year, 1, 1)]

			if 'Value' in quandl_data and 'Close' not in quandl_data:
				quandl_data = quandl_data.rename(columns={'Value': 'Close'})

			#need to rename volume column
			try:
				volume_column = quandl_data.columns[['volu' in col.lower() for col in quandl_data.columns]][0]
				quandl_data = quandl_data.rename(columns={volume_column:'Volume'})
			except:
				pass

			for column in ['High', 'Low', 'Open', 'Volume']:
				if column not in quandl_data:
					quandl_data[column] = None

			#load into contracts
			contract = Contract(seriesid=self.id,
								cusip=self.ticker)

			contract_db = Contract.filter(Contract.cusip==self.ticker).first()

			if contract_db:
				contract= contract_db
			else:
				contract.save()

			#pull daily data
			quandl_data['dt'] = quandl_data.index
			quandl_data['contractid'] = contract.id
			quandl_data['contract_num'] = 1
			quandl_data['open_'] = quandl_data.Open
			quandl_data['high'] = quandl_data.High
			quandl_data['low'] = quandl_data.Low
			quandl_data['close_'] = quandl_data.Close
			quandl_data['volume'] = quandl_data.Volume
			quandl_data['chg'] = quandl_data.close_.diff(1)

			history_dat = history_dat.append(quandl_data)

		elif self.type == 'FUT':
			#insert data into Daily
			while year:
				for delivery_code in self.delivery:
					#quandl
					quandl_code = '{0}{1}{2}'.format(self.quandl_code, delivery_code, year)
					cusip = '{0}{1}{2}'.format(self.ticker, delivery_code, year)

					print 'Loading {0}'.format(cusip)

					#ibcontract
					ibcontract = IBcontract()
					ibcontract.secType = str(self.type)
					ibcontract.exchange = str(self.exchange)
					ibcontract.symbol = str(self.symbol)

					if self.type == 'FUT':
						ibcontract.expiry = str(year) + "%02d" % FUT_CONTRACT_CODES[delivery_code]
						# ibcontract.localSymbol = str('{0}{1}{2}'.format(self.ticker, delivery_code, str(year)[-1]))
						ibcontract.localSymbol = str('{0} {1} {2}'.format(self.ticker, datetime.date(1900, FUT_CONTRACT_CODES[delivery_code], 1).strftime('%b'), str(year)[-2:]).upper())
					try:
						try:
							quandl_data = Quandl.get(quandl_code, authtoken=QUANDL_API_KEY)
						except:
							from time import sleep
							sleep(10)
							quandl_data = Quandl.get(quandl_code, authtoken=QUANDL_API_KEY)

						#need to rename volume column
						volume_column = quandl_data.columns[['volu' in col.lower() for col in quandl_data.columns]][0]
						quandl_data = quandl_data.rename(columns={volume_column:'Volume'})

						quandl_data = quandl_data[quandl_data.Volume > 0]

						#quandl expiry
						expiry_dt = quandl_data.iloc[-1].name
						roll_dt = expiry_dt - datetime.timedelta(days=self.roll_days)



						#load into contracts
						contract = Contract(seriesid=self.id,
											cusip=cusip,
											expiry_month=ibcontract.expiry,
											roll=roll_dt,
											expiry_dt=expiry_dt)

						contract_db = Contract.filter(Contract.cusip==cusip).first()
						if contract_db:
							expiry_dt = contract_db.expiry_dt
							roll_dt = expiry_dt - datetime.timedelta(days=self.roll_days)

						#check ibcontract expiry
						if (today - expiry_dt).days <= 5 and not contract_db:# and today < expiry_dt:

							ibcontract.expiry = ""
							contract.expiry_dt = None
							ibcontract.localSymbol = contract.local_cusip

							try:
								details = client.get_contract_details(ibcontract)
								expiry_dt = datetime.datetime.strptime(details['expiry'], '%Y%m%d')
								roll_dt = expiry_dt - datetime.timedelta(days=self.roll_days)
								contract.expiry_dt = expiry_dt
								contract.roll = roll_dt
							except:
								contract.expiry_dt = expiry_dt
								contract.roll_dt = roll_dt
								# contract.save()

								continue

						if not contract_db:
							contract.save()
						else:
							contract = contract_db


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
		print 'about to insert'

		dat = history_dat[['contractid', 'dt', 'contract_num', 'open_', 'high', 'low', 'close_', 'volume', 'chg']]
		dat_dict = dat.to_dict(orient='records')
		Daily.__table__.insert().execute(dat_dict)

	def update_atr(self, client):
		contract_num = 1
		while True:
			try:
				df = self.data(contract_num=contract_num, filter_forward=False)
			except ValueError:
				break

			print 'Updating ATR10 for contract {0}'.format(contract_num)

			try:
				df['settlementYest'] = df.close_ - df.chg
				df['tr'] = pandas.DataFrame([df.high - df.low, numpy.abs(df.high - df.settlementYest), numpy.abs(df.low - df.settlementYest)]).max()
				df['atr10'] = talib.SMA(df.tr.values, timeperiod=10)
			except:
				break

			#wilder smoothing
			for i in xrange(10, len(df)):
				try:
					df.ix[i, 'atr10'] = (df.ix[i-1, 'atr10'] * 9.0 + df.ix[i, 'tr']) / 10.0
				except:
					df.ix[i, 'atr10'] = df.ix[i-1, 'atr10']

			df = df.rename(columns={'id':'_id'})
			stmt = Daily.__table__.update().where(Daily.__table__.c.id == bindparam('_id')).values({'atr10': bindparam('atr10'),})
			update_dict = df[['_id', 'atr10']].to_dict(orient='records')
			engine.connect().execute(stmt, update_dict)

			contract_num += 1

			if contract_num > 12:
				break

	def update_contracts(self, client):
		if self.type <> 'FUT':
			print 'Only valid for Futures series'
			return None

		latest_contract = Contract.filter(Contract.seriesid == self.id).order_by(desc(Contract.expiry_dt)).first()

		delivery_code = latest_contract.cusip.replace(self.ticker, '')[0]
		delivery_year = int(latest_contract.cusip[-4:])

		#ibcontract
		ibcontract = IBcontract()
		ibcontract.secType = str(self.type)
		ibcontract.exchange = str(self.exchange)
		ibcontract.symbol = str(self.symbol)

		delivery_index = self.delivery.find(delivery_code)
		next_delivery_index =(delivery_index + 1) % len(self.delivery)
		next_delivery_code = self.delivery[next_delivery_index]

		if next_delivery_index > delivery_index:
			next_delivery_year = delivery_year
		else:
			next_delivery_year = delivery_year + 1


		ibcontract.expiry = str(next_delivery_year) + "%02d" % FUT_CONTRACT_CODES[next_delivery_code]
		ibcontract.localSymbol = str('{0}{1}{2}'.format(self.ticker, next_delivery_code, str(next_delivery_year)[-1]))

		details = client.get_contract_details(ibcontract)

		if not details:
			ibcontract.localSymbol = str('{0} {1} {2}'.format(self.ticker, datetime.date(1900, FUT_CONTRACT_CODES[next_delivery_code], 1).strftime('%b'), str(next_delivery_year)[-2:]).upper())
			details = client.get_contract_details(ibcontract)

		if details:
			contract = Contract()
			contract.seriesid = self.id
			contract.cusip = '{0}{1}{2}'.format(self.ticker, next_delivery_code, next_delivery_year)
			contract.expiry_month = ibcontract.expiry
			contract.expiry_dt = datetime.datetime.strptime(details['expiry'], '%Y%m%d')
			contract.roll = contract.expiry_dt - datetime.timedelta(days=self.roll_days)

			try:
				contract.save()
			except:
				session.rollback()

			print 'Inserted New Contract: {0}'.format(contract.cusip)
		else:
			pass



class Contract(Base):
	__tablename__ = 'Contract'

	id = Column(Integer, primary_key=True)
	seriesid = Column(Integer, ForeignKey('Series.id'))
	cusip = Column(String(30), nullable=False)

	roll = Column(DateTime, nullable=True)
	first_notice = Column(DateTime, nullable=True)
	expiry_month = Column(Integer, nullable=True)
	expiry_dt = Column(DateTime, nullable=True)

	__table_args__ = (UniqueConstraint('seriesid', 'cusip'),
					)

	@property
	def series(self):
		#get series
		series = Series.filter(Series.id == self.seriesid).first()
		return series

	@property
	def spread(self):
		if self.series.type == 'SPD':
			long_cusip, short_cusip = self.cusip.split('/')
			long_leg = self.__class__.filter(self.__class__.cusip == long_cusip).first()
			short_leg = self.__class__.filter(self.__class__.cusip == short_cusip).first()
			return Spread(long_leg, short_leg)
		else:
			print 'Error not spread'
			return None

	@property
	def local_cusip(self):
		series = Series.filter(Series.id == self.seriesid).first()

		if self.cusip:
			delivery_code = self.cusip.replace(series.ticker, '')[0]
			year = self.cusip[-4:]
			if series.local_code == 2:
				return str('{0} {1} {2}'.format(series.ticker, datetime.date(1900, FUT_CONTRACT_CODES[delivery_code], 1).strftime('%b'), str(year)[-2:]).upper())
			elif series.local_code == 3:
				return str('{0}   {1} {2}'.format(series.ticker, datetime.date(1900, FUT_CONTRACT_CODES[delivery_code], 1).strftime('%b'), str(year)[-2:]).upper())
			else:
				return str('{0}{1}{2}'.format(series.ticker, delivery_code, str(year)[-1]))
		elif self.expiry_dt:
			if series.local_code == 2:
				return str('{0} {1} {2}'.format(series.ticker, datetime.date(1900, self.expiry_dt.month, 1).strftime('%b'), str(self.expiry_dt.year)[-2:]).upper())
			elif series.local_code == 3:
				return str('{0}   {1} {2}'.format(series.ticker, datetime.date(1900, self.expiry_dt.month, 1).strftime('%b'), str(self.expiry_dt.year)[-2:]).upper())
			else:
				return str('{0}{1}{2}'.format(series.ticker, {v: k for k, v in FUT_CONTRACT_CODES.items()}[self.expiry_dt.month], str(self.expiry_dt.year)[-1]))
		else:
			print 'Unable to create local cusip'
			return None

	@property
	def contract(self):
		#get series
		series = Series.filter(Series.id==self.seriesid).first()

		#create contract
		ibcontract = IBcontract()
		ibcontract.symbol = str(series.symbol)
		ibcontract.secType = str(series.type)
		ibcontract.exchange = str(series.exchange)

		if series.type == 'FUT':
			# ibcontract.expiry = self.expiry_dt.strftime('%Y%m%d')
			ibcontract.localSymbol = self.local_cusip
		if series.type == 'CASH':
			ibcontract.currency = 'USD'

		return ibcontract

	@property
	def dte(self):
		return (self.expiry_dt.date() - fut_date(datetime.datetime.now())).days

	@property
	def atr10(self):
		yest = Daily.filter(and_(Daily.contractid==self.id, Daily.dt< fut_date(datetime.datetime.now()))).order_by(desc(Daily.dt)).first()

		return yest.atr10

	@property
	def last_exchange(self):
		series = self.series
		if series.currency == 'USD':
			return 1.0
		else:
			currency = Series.filter(Series.ticker == series.currency).first().front_contract
			yest_close = Daily.filter(Daily.contractid == currency.id).order_by(Daily.dt.desc()).first().close_
			return yest_close

	def settle_yest(self, client, strategy=None):
		if strategy:
			today = fut_date(strategy.dt)

			yest = Daily.filter(and_(Daily.contractid==self.id, Daily.dt < today)).order_by(Daily.dt.desc()).first()
			return yest.close_
		else:
			yest = self.hist_data(client).iloc[-2]
			return yest.close

	def market_data(self, client, num_times=5, strategy=None):
		if strategy:
			if not strategy.live:
				if strategy.intradaybt:
					minute = Intraday.filter(and_(Intraday.contractid==self.id, Intraday.dt==strategy.dt)).first()
					market_data_dict = {'bid':0, 'ask':0, 'bid_px': minute.open_, 'ask_px': minute.open_, 'last': minute.open_}
				else:
					minute = Daily.filter(and_(Daily.contractid==self.id, Daily.dt==datetime.datetime.combine(strategy.today, datetime.time.min))).first()
					market_data_dict = {'bid':0, 'ask':0, 'bid_px': minute.close_, 'ask_px': minute.close_, 'last': minute.close_}

				return market_data_dict

		for i in xrange(num_times):
			market_data = client.get_IB_market_data(self.contract, seconds=1.0)

			if numpy.nan not in market_data:
				market_data_dict = {a:b for a,b in zip(('bid', 'ask', 'bid_px', 'ask_px', 'last'), market_data)}
				return market_data_dict

		#try from hist data
		last_min = self.hist_data(client, durationStr="60 S", barSizeSetting="1 min")
		last_close = last_min.iloc[-1].close
		market_data_dict = {'bid':0, 'ask':0, 'bid_px': last_close, 'ask_px': last_close, 'last': last_close}
		return market_data_dict

		#otherwise
		raise Exception('No data from market data feed')

	def market_data_yahoo(self):
		if self.series.type == 'IND':
			ticker = self.series.quandl_code.replace('YAHOO/', '').replace('INDEX_', '^')
			response = urllib2.urlopen('http://finance.yahoo.com/d/quotes.csv?s={0}&f=snl1'.format(ticker))
			csv = response.read()
			px = float(csv.split(',')[2])
			return px
		else:
			raise Exception('Yahoo market data only for Indices')

	def hist_data(self, client, durationStr="5 D", barSizeSetting="1 day", endDateTime=None):
		hist_data = client.get_IB_historical_data(self.contract, durationStr, barSizeSetting, endDateTime)

		return hist_data

	def minute_data(self, client, dt, strategy=None):
		if strategy:
			if not strategy.live:
				if strategy.intradaybt:
					minute = Intraday.filter_df(and_(Intraday.contractid==self.id, Intraday.dt==convert_EST(dt.replace(tzinfo=None), str(dt.tzinfo))))
					minute = minute.rename(columns={'open_':'open', 'close_':'close'})

				else:
					#returns close price from daily
					minute = Daily.filter(and_(Daily.contractid==self.id, Daily.dt==datetime.datetime.combine(strategy.today, datetime.time.min))).first()
					minute = pandas.DataFrame({'seriesid': self.seriesid, 'contractid': self.id, 'open': minute.close_, 'high': minute.close_, 'low':minute.close_, 'close': minute.close_}, index=[0])

				return minute.iloc[0]

		data = self.hist_data(client, durationStr="120 S", barSizeSetting="1 min", endDateTime=(dt + datetime.timedelta(seconds=60)).replace(tzinfo=None))

		return data.iloc[-1]

	def contract_details(self, client):
		contract_details = client.get_contract_details(self.contract)

		return contract_details

	def place_order(self, trade_amt, strategyid, type, trade_px=None, starttime=None, endtime=None, target=None, callback=None):
		if type is not 'MKT' and not trade_px:
			print "Warning: OrderType needs trade_px. No Order placed."
			return None

		if trade_amt:
			trade_amt = int(trade_amt)
		if trade_px:
			trade_px = self.series.nearest_tick(float(trade_px))

		order = Order(contractid=self.id,
						ib_id=None,
						starttime=starttime,
						endtime=endtime,
						trade_amt=trade_amt,
						trade_px=trade_px,
						type=type,
						strategyid=int(strategyid),
						target=target,
					  	callback=callback)

		order.save()

		return order.id

	def place_order2(self, trade_amt, strategy, type, trade_px=None, starttime=None, endtime=None, target=None, callback=None):
		if type is not 'MKT' and not trade_px:
			print "Warning: OrderType needs trade_px. No Order placed."
			return None

		if trade_amt:
			trade_amt = int(round(trade_amt))
		if trade_px:
			trade_px = self.series.nearest_tick(float(trade_px))

		if strategy.live == False:
			starttime = starttime.astimezone(pytz.timezone('US/Eastern'))
			endtime = endtime.astimezone(pytz.timezone('US/Eastern'))

			strategy.orders.append({'contractid':self.id,
						'ib_id':None,
						'starttime':starttime,
						'endtime':endtime,
						'trade_amt':trade_amt,
						'trade_px':trade_px,
						'type':type,
						'strategyid':int(strategy.id),
						'target':target,
						'callback':callback})

		else:
			order = Order(contractid=self.id,
							ib_id=None,
							starttime=starttime,
							endtime=endtime,
							trade_amt=trade_amt,
							trade_px=trade_px,
							type=type,
							strategyid=int(strategy.id),
							target=target,
							callback=callback)

			order.save()

			return order.id

class Spread(Contract):
	def __init__(self, long_leg, short_leg):
		self.long_leg = long_leg
		self.short_leg = short_leg

	def contract(self, client):

		series = self.long_leg.series
		#create contract
		ibcontract = IBcontract()
		ibcontract.symbol = str(series.symbol)
		ibcontract.secType = "BAG"
		ibcontract.currency = str(series.currency)
		ibcontract.exchange = str(series.exchange)


		leg1 = ComboLeg()
		leg1.conId = client.get_contract_details(self.long_leg.contract)['conId']
		leg1.ratio = 1
		leg1.action = "BUY"
		leg1.exchange = str(series.exchange)

		leg2 = ComboLeg()
		leg2.conId = client.get_contract_details(self.short_leg.contract)['conId']
		leg2.ratio = 1
		leg2.action = "SELL"
		leg2.exchange = str(series.exchange)

		ibcontract.comboLegs = ComboLegList([leg1, leg2])
		return ibcontract

	def place_order2(self, trade_amt, strategy, type, trade_px=None, starttime=None, endtime=None, target=None, callback=None):
		if type is not 'MKT' and not trade_px:
			print "Warning: OrderType needs trade_px. No Order placed."
			return None

		if trade_amt:
			trade_amt = int(round(trade_amt))
		if trade_px:
			trade_px = self.series.nearest_tick(float(trade_px))

		if strategy.live == False:
			starttime = starttime.astimezone(pytz.timezone('US/Eastern'))
			endtime = endtime.astimezone(pytz.timezone('US/Eastern'))

			#place two orders
			strategy.orders.append({'contractid':self.long_leg.id,
						'ib_id':None,
						'starttime':starttime,
						'endtime':endtime,
						'trade_amt':trade_amt,
						'trade_px':trade_px,
						'type':type,
						'strategyid':int(strategy.id),
						'target':target,
						'callback':callback})

			strategy.orders.append({'contractid':self.short_leg.id,
						'ib_id':None,
						'starttime':starttime,
						'endtime':endtime,
						'trade_amt':-trade_amt,
						'trade_px':trade_px,
						'type':type,
						'strategyid':int(strategy.id),
						'target':target,
						'callback':callback})

		else:
			cusip = '{0}/{1}'.format(self.long_leg.cusip, self.short_leg.cusip)
			calspd = Contract.filter(Contract.cusip == cusip).first()

			if calspd is None:
				series = Series.filter(Series.symbol == 'CALSPD').first()
				calspd = Contract()
				calspd.seriesid = series.id
				calspd.cusip = cusip
				calspd.save()


			# self.front_conid = strategy.client.get_contract_details(self.front_leg.contract)['underConId']
			# self.back_conid = strategy.client.get_contract_details(self.back_leg.contract)['underConId']


			order = Order(contractid=calspd.id,
							ib_id=None,
							starttime=starttime,
							endtime=endtime,
							trade_amt=trade_amt,
							trade_px=trade_px,
							type=type,
							strategyid=int(strategy.id),
							target=target,
							callback=callback)

			order.save()

			return order.id







class Order(Base):
	__tablename__ = 'Order'
	id = Column(Integer, primary_key=True)
	dt = Column(DateTime, nullable=False, default=datetime.datetime.now)
	starttime = Column(DateTime, nullable=False, default=datetime.datetime.now)
	endtime = Column(DateTime, nullable=False, default=now_offset) #trade window in seconds
	contractid = Column(Integer, ForeignKey('Contract.id'))
	ib_id = Column(Integer, nullable=True)
	trade_amt = Column(Integer, nullable=False)
	trade_px = Column(Float, default=0.0)
	type = Column(String(250), nullable=False)

	strategyid = Column(Integer, ForeignKey('Strategy.id'))
	target = Column(Boolean, nullable=False, default=False)
	callback = Column(String(250), nullable=True)

class Fill(Base):
	__tablename__ = 'Fill'
	id = Column(Integer, primary_key=True)
	dt = Column(DateTime, nullable=False, default=datetime.datetime.now)
	contractid = Column(Integer, ForeignKey('Contract.id'))
	orderid = Column(Integer, ForeignKey('Order.id'))
	trade_amt = Column(Integer, nullable=False)
	trade_px = Column(Float, nullable=False)

	@classmethod
	def current_positions(cls):
		all_fills = cls.filter_df(cls.id > 0)

		current_positions = all_fills.groupby('contractid').sum()
		current_positions['contractid'] = current_positions.index

		non_zero = current_positions[current_positions.trade_amt <> 0][['contractid', 'trade_amt']]
		return non_zero

class Order_Backtest(Base):
	__tablename__ = 'Order_Backtest'
	id = Column(Integer, primary_key=True)
	dt = Column(DateTime, nullable=False, default=datetime.datetime.now)
	starttime = Column(DateTime, nullable=False, default=datetime.datetime.now)
	endtime = Column(DateTime, nullable=False, default=now_offset) #trade window in seconds
	contractid = Column(Integer, ForeignKey('Contract.id'))
	ib_id = Column(Integer, nullable=True)
	trade_amt = Column(Integer, nullable=False)
	trade_px = Column(Float, default=0.0)
	type = Column(String(250), nullable=False)

	strategyid = Column(Integer, ForeignKey('Strategy.id'))
	target = Column(Boolean, nullable=False, default=False)
	callback = Column(String(250), nullable=True)

class Fill_Backtest(Base):
	__tablename__ = 'Fill_Backtest'
	id = Column(Integer, primary_key=True)
	dt = Column(DateTime, nullable=False, default=datetime.datetime.now)
	contractid = Column(Integer, ForeignKey('Contract.id'))
	orderid = Column(Integer, ForeignKey('Order.id'))
	trade_amt = Column(Integer, nullable=False)
	trade_px = Column(Float, nullable=False)

	@classmethod
	def current_positions(cls):
		all_fills = cls.filter_df(cls.id > 0)

		current_positions = all_fills.groupby('contractid').sum()
		current_positions['contractid'] = current_positions.index

		non_zero = current_positions[current_positions.trade_amt <> 0][['contractid', 'trade_amt']]
		return non_zero

class StrategyLog(Base):
	__tablename__ = 'StrategyLog'
	id = Column(Integer, primary_key=True)
	dt = Column(DateTime, nullable=False, default=datetime.datetime.now)
	strategyid = Column(Integer, ForeignKey('Strategy.id'))
	status = Column(Boolean, nullable=False)
	msg = Column(String(250), nullable=False)

	@classmethod
	def log(cls, strategy, status, message=None):
		strategylog = cls()
		strategylog.strategyid = strategy.id
		strategylog.status = status

		if message:
			strategylog.msg = message
		else:
			strategylog.msg = str(strategy.params)

		if status is False:
			#email/text
			email = Email("{0} Strategy {1} Failed".format(datetime.datetime.now(), strategy.id), message)
			email.send(EMAIL_TEXT)

		strategylog.save()
		return strategylog.id

class Strategy(Base):
	__tablename__ = 'Strategy'
	id = Column(Integer, primary_key=True)
	name = Column(String(250), nullable=False)
	description = Column(String(250), nullable=False)
	start_time = Column(Time, nullable=False)
	end_time = Column(Time, nullable=True)
	scalar = Column(Float, nullable=False)
	interval = Column(Interval, nullable=True)
	tz = Column(String(250), nullable=False) #US/Eastern
	continuous = Column(Boolean, nullable=False)
	paper = Column(Boolean, nullable=False, default=0)
	discriminator = Column('type', String)
	universe = Column(String(250), nullable=True)
	intraday = Column(Boolean, nullable=True)

	__mapper_args__ = {'polymorphic_on': discriminator}

	def __init__(self):
		self.intradaybt=True

	@property
	def fills(self):
		colnames = ['dt', 'contractid', 'trade_amt', 'trade_px', 'cusip', 'type']
		fills = pandas.DataFrame(session.query(Fill.dt, Fill.contractid, Fill.trade_amt, Fill.trade_px, Contract.cusip, Order.type) \
								 .join(Order).join(Contract, Fill.contractid==Contract.id) \
								 .filter(Order.strategyid==self.id).all(), columns=colnames)

		fills['strategy'] = self.name
		fills['fut_date'] = fills.dt.map(lambda a: fut_date(a))
		return fills[['strategy', 'dt', 'fut_date', 'cusip', 'type', 'contractid', 'trade_amt', 'trade_px']]

	@property
	def positions(self):
		fills = self.fills

		positions = fills.groupby(['contractid', 'cusip'], as_index=False).sum()
		positions['strategy'] = self.name

		if len(positions):
			return positions[['strategy', 'cusip', 'contractid', 'trade_amt']]
		else:
			return None


	@property
	def pnls(self):
		daily_data = session.query(Fill.dt,  Fill.contractid, Fill.trade_amt, Fill.trade_px)\
								.join(Order)\
								.filter(Order.strategyid==self.id).all()

		if not daily_data:
			return pandas.DataFrame()

		daily_data = pandas.DataFrame(daily_data).sort_values('dt')
		daily_data = daily_data.rename(columns={'dt': 'dt_fill'})

		daily_data['dt'] = pandas.to_datetime(daily_data.dt_fill.map(lambda a: fut_date(a)))

		#pull all days
		traded_contracts = daily_data.contractid.unique().astype('int')
		first_dt = daily_data.dt.min().date()
		px_data = pandas.DataFrame(session.query(Daily.dt, Daily.contractid, Daily.close_, Daily.chg, Contract.cusip, Series.cupp, Series.currency)\
								.join(Contract).join(Series)\
								.filter(Daily.contractid.in_(traded_contracts), Daily.dt>=first_dt).all())
		fx_data = pandas.DataFrame(session.query(Daily.dt, Series.ticker.label('currency'), Daily.close_.label('fx')).join(Contract).join(Series).filter(Series.type=='CASH', Daily.dt>=first_dt).all())

		all_days = px_data.merge(fx_data, on=['dt', 'currency'], how='left')
		all_days['fx'] = all_days.fx.fillna(1)

		#full outer join two tables
		daily_data = daily_data.merge(all_days, on=['dt', 'contractid'], how='outer').sort_values(['dt', 'dt_fill'])

		bod_position = [None] * len(daily_data)
		for i in xrange(len(daily_data)):

			row = daily_data.iloc[i]
			prior_fills = daily_data.iloc[:i]

			prior_fills = prior_fills[(prior_fills.dt < row['dt'].date()) & (prior_fills.contractid == row['contractid'])]
			bod_position[i] = prior_fills['trade_amt'].sum()

		daily_data['bod_position'] = bod_position
		daily_data['trading_pnl'] = (daily_data.close_ - daily_data.trade_px) * daily_data.trade_amt * daily_data.cupp * daily_data.fx
		daily_data['widening_pnl'] = (daily_data.chg) * daily_data.bod_position * daily_data.cupp * daily_data.fx

		daily_data.trading_pnl = daily_data.trading_pnl.fillna(0)
		daily_data.widening_pnl = daily_data.widening_pnl.fillna(0)
		daily_data['pnl'] = daily_data.trading_pnl + daily_data.widening_pnl
		return daily_data

	def _sync_to_db(f, **params):
		def sync(self, **params):
			if self.name:
				strategy = Strategy.filter(Strategy.name == self.name).first()
			else:
				strategy = Strategy.filter(Strategy.name == type(self).__name__).first()

			if strategy:
				self.name = strategy.name
				self.description = strategy.description
				self.start_time = strategy.start_time
				self.end_time = strategy.end_time
				self.scalar = strategy.scalar
				self.interval = strategy.interval
				self.tz = strategy.tz
			else:
				raise Exception("Strategy not saved in Strategy DB Table")

			return f(self, **params)
		return sync

	def _run_check(f, **params):
		def run_check(self, **params):
			if self._check():
				f(self, **params)
			else:
				print "Check failed"
		return run_check

	@_sync_to_db
	@_run_check
	def _trade(self, **params):
		self.trade(**params)

	@_sync_to_db
	def _check(self):
		result = self.check()
		return result

	#overwrite this
	def compute(self):
		pass

	#overwrite this
	def trade(self):
		pass

	#overwrite this
	def check(self):
		pass

	def summary(self):
		pass

	@_sync_to_db
	def run(self, **params):
		self.params = {}
		self.today = fut_date(datetime.datetime.now())
		self.live=True
		self.dt = datetime.datetime.now()
		self.dm=DataManager(self.compute(**params), self)

		local_tz = get_localzone()
		now_local = datetime.datetime.now(local_tz)

		now_strategy = now_local.astimezone(pytz.timezone(self.tz))

		if self.continuous and self.end_time:
			end_strategy = datetime.datetime.combine(now_strategy.date(), self.end_time)
			end_strategy = pytz.timezone(self.tz).localize(end_strategy)
			end_local = end_strategy.astimezone(local_tz)

		attempts = 0

		while True:
			try:
				self._trade(**params)
				StrategyLog.log(self, status=True)
				break
			except Exception as e:
				traceback.print_exc()
				StrategyLog.log(self, status=False, message=str(e))

			attempts += 1
			sleep(3)
			if attempts >= 3:
				break

	def fill_px(self, trade, row, slippage=True):
		px = None

		if row.type == 'MKT':
			px = row.open_ + 0.5 * numpy.sign(trade) * row.tick * slippage
		elif row.type == 'LMT':
			if trade > 0:
				if row.open_ < row.trade_px:
					px = row.open_ + 0.5 * numpy.sign(trade) * row.tick * slippage
				elif row.low < row.trade_px:
					px = row.trade_px
				else:
					pass
			if trade < 0:
				if row.open_ > row.trade_px:
					px = row.open_ - 0.5 * numpy.sign(trade) * row.tick * slippage
				elif row.high > row.trade_px:
					px = row.trade_px
				else:
					pass
		elif (row.type == 'STOP') or (row.type == 'STOPC'):
			if trade > 0:
				if row.open_ >= row.trade_px:
					px = row.open_ + 0.5 * numpy.sign(trade) * row.tick * slippage
				elif row.high >= row.trade_px:
					px = row.trade_px + 0.5 * numpy.sign(trade) * row.tick * slippage
				else:
					pass
			if trade < 0:
				if row.open_ <= row.trade_px:
					px = row.open_ - 0.5 * numpy.sign(trade) * row.tick * slippage
				elif row.low <= row.trade_px:
					px = row.trade_px - 0.5 * numpy.sign(trade) * row.tick * slippage
				else:
					pass
		else:
			pass

		return px

	def simulate_trades(self, fills, slippage=True):
		contractids = fills.contractid.unique()

		out = pandas.DataFrame()

		for contractid in contractids:
			px = fills[fills.contractid==contractid].sort_values(by='starttime')
			px['dt'] = px.starttime.map(lambda a: a.date())
			current_position = 0
			bod_position = [0] * len(px)
			positions = [0] * len(px)
			trades = [0] * len(px)
			fill_pxs = [0] * len(px)

			for i in xrange(len(px)):
				row = px.iloc[i]

				#simulate fill px
				if row.target:
					trade = row.trade_amt - current_position
				else:
					trade = row.trade_amt

				fill_px = self.fill_px(trade, row, slippage)

				fill_pxs[i] = fill_px

				trades[i] = trade
				if fill_px:
					current_position = current_position + trade
				positions[i] = current_position

				if i > 0 and px.dt.iloc[i-1] <> px.dt.iloc[i]:
					bod_position[i] = positions[i-1]

			px['fill_px'] = fill_pxs
			px['trade'] = trades
			px['position'] = positions
			px['bod_position'] = bod_position

			out = out.append(px)

		return out

	def simulate_callbacks(self, fills):
		callbacks = fills[fills.callback.notnull()]

		for i in xrange(len(callbacks)):
			orderid = callbacks.iloc[i].orderid

			order = Order_Backtest.filter(Order_Backtest.id==int(orderid)).first()

			self.dt = order.starttime
			self.today = fut_date(order.starttime)

			cb = getattr(self, callbacks.iloc[i].callback)

			cb(order, Order_Backtest, Fill_Backtest)

	def simulate_fills(self, callback=True, slippage=True):
		if self.intradaybt:
			daily_query = session.query(Daily, Contract.seriesid)\
						.join(Contract).filter(Daily.contractid.in_(session.query(Order_Backtest.contractid).distinct())).subquery()

			px_query = session.query(Order_Backtest, Intraday.open_, Intraday.high, Intraday.low, Intraday.close_)\
						.join(Intraday, and_(Order_Backtest.contractid==Intraday.contractid,
											 Intraday.dt.between(Order_Backtest.starttime, Order_Backtest.endtime))).subquery()

			query = session.query(px_query, daily_query.c.close_.label('settlement'), daily_query.c.chg, daily_query.c.contract_num, Series.cupp, Series.tick, Series.id.label('seriesid'))\
						.join(daily_query, and_(px_query.c.contractid == daily_query.c.contractid,
												func.date(px_query.c.starttime) == func.date(daily_query.c.dt)))\
						.outerjoin(Series,daily_query.c.seriesid==Series.id)
			px = pandas.read_sql(query.statement, query.session.bind)
		else:
			daily_query = session.query(Daily, Contract.seriesid).join(Contract).filter(Daily.contractid.in_(session.query(Order_Backtest.contractid).distinct())).subquery()

			query = session.query(Order_Backtest, daily_query.c.close_.label('settlement'), daily_query.c.chg, daily_query.c.contract_num, Series.cupp, Series.tick, Series.id.label('seriesid')).join(daily_query, and_(Order_Backtest.contractid == daily_query.c.contractid, func.date(Order_Backtest.starttime) == func.date(daily_query.c.dt))).outerjoin(Series, daily_query.c.seriesid==Series.id)
			px = pandas.read_sql(query.statement, query.session.bind)

			px['open_'] = px.settlement
			px['high'] = px.settlement
			px['low'] = px.settlement
			px['close_'] = px.settlement


		#filter missing orderids
		missing_orders = set(px.id.unique()) ^ set(range(1,px.id.max()+1))
		for order_id in missing_orders:
			order = Order_Backtest.filter(Order_Backtest.id==order_id).first()
			date = fut_date(order.starttime)
			contractid = order.contractid

			px = px[~((px.starttime.map(lambda a: fut_date(a)) == date) & (px.contractid == contractid))]

		groupby = px.groupby('id')
		fills = groupby.first()
		fills['high'] = groupby.high.max()
		fills['low'] = groupby.low.min()
		fills['close_'] = groupby.close_.last()
		fills['fill_px'] = None
		fills['orderid'] = fills.index

		fills = self.simulate_trades(fills, slippage)


		fills_upload = fills[['starttime', 'contractid', 'orderid', 'trade', 'fill_px']].dropna().rename(columns = {'starttime':'dt', 'trade': 'trade_amt', 'fill_px': 'trade_px'})

		Fill_Backtest.__table__.insert().execute(fills_upload.to_dict(orient='records'))

		if fills.callback.notnull().sum() > 0 and callback:
			self.simulate_callbacks(fills)

			return None

		return fills

	def simulate_orders(self, series_info, bt_start, intraday=True):
		est = pytz.timezone('US/Eastern')
		bt_end = fut_date(datetime.datetime.now())
		self.today = bt_start
		self.params = {}
		#init data manager
		self.dm = DataManager(self.compute(**series_info.__dict__), self)


		while self.today < bt_end:
			start_time_tz = datetime.datetime.combine(self.today, series_info.starttime)
			start_time_tz = pytz.timezone(series_info.tz).localize(start_time_tz)
			end_time_tz = datetime.datetime.combine(self.today, series_info.endtime)
			end_time_tz = pytz.timezone(series_info.tz).localize(end_time_tz)
			start_time_local = start_time_tz.astimezone(est)
			end_time_local = end_time_tz.astimezone(est)

			times = [start_time_local]
			#pull times
			for dt in times:
				self.dt = dt

				if intraday:
					try:
						self.trade(**series_info.__dict__)
						# print '{0}'.format(self.today)
					except KeyboardInterrupt:
						raise Exception('Manually Exited')
					except:
						pass

					self.today = self.today + datetime.timedelta(days=1)


	def backtest(self, bt_start=datetime.date(2010,1,1), intraday=True, slippage=True):
		try:
			exec('from strategy import {0}'.format(self.universe)) in globals(), locals()
		except:
			print 'No universe file found'

		self.live = False
		self.intradaybt = intraday

		#clear old backtest data
		session.query(Fill_Backtest).delete()
		session.query(Order_Backtest).delete()
		session.commit()

		# self.orders = clone_table('{0}_orders'.format(self.name), Order.__table__, Base.metadata)
		# self.orders.create()



		all_fills = pandas.DataFrame()
		all_orders = pandas.DataFrame()
		if self.universe:
			Universe = eval(self.universe)
			for series_info in Universe.filter(Universe.id > 0).all():
				self.orders = []
				print 'Simulating UniverseId: {0}'.format(series_info.id)
				self.simulate_orders(series_info, bt_start=bt_start)

				Order_Backtest.__table__.insert().execute(self.orders)

				print 'Preparing Fills'
				#simulating fills

				fills = self.simulate_fills(slippage=slippage)

				session.query(Order_Backtest).delete()
				Order_Backtest.__table__.insert().execute(self.orders)

				if fills is None:
					fills = self.simulate_fills(callback=False, slippage=slippage)

				all_fills = all_fills.append(fills)

				all_orders = pandas.DataFrame(self.orders).append(all_orders)

				session.query(Fill_Backtest).delete()
				session.query(Order_Backtest).delete()
				session.commit()

			if intraday:
				all_fills['trading_pnl'] = all_fills.trade * (all_fills.settlement - all_fills.fill_px) * all_fills.cupp * 1.0
			else:
				all_fills['trading_pnl'] = all_fills.trade * (all_fills.settlement - all_fills.settlement) * all_fills.cupp * 1.0

			all_fills['widening_pnl'] = all_fills.bod_position * all_fills.cupp * all_fills.chg
			all_fills['pnl'] = all_fills.trading_pnl + all_fills.widening_pnl
			all_fills['dt'] = all_fills.starttime.map(lambda a: a.date())

			import matplotlib.pyplot as plt
			bt_pnl = all_fills.groupby(['dt', 'seriesid'], as_index=False).sum()[['dt', 'seriesid', 'pnl']]

			Order_Backtest.__table__.insert().execute(all_orders.to_dict(orient='records'))
			fills_upload = all_fills[['starttime', 'contractid', 'orderid', 'trade', 'fill_px']].rename(columns={'starttime':'dt', 'trade':'trade_amt', 'fill_px':'trade_px'})
			Fill_Backtest.__table__.insert().execute(fills_upload.dropna().to_dict(orient='records'))

			# s4_bt.pivot('dt', 'seriesid').fillna(0).cumsum().plot()
			# s4_bt.pivot('dt', 'seriesid').fillna(0).sum(1).cumsum().plot()


			return bt_pnl
