from flask import Flask
from flask.ext.script import Manager, Shell
from ibclient.models import *
from copy import deepcopy
from statsmodels.tsa.ar_model import AR
import rpy2.robjects as robjects
from rpy2.robjects.numpy2ri import numpy2ri
from rpy2.robjects.packages import importr

import db.models as models
import matplotlib.pyplot as plt
from db.models.trading import *
from db.models.data import *
from db.sqlite import *
from strategy import *


# from strategy.s1 import S1
# from strategy.s2 import S2
# from strategy.s3 import S3

importr('forecast')

app = Flask(__name__)
app.debug = True

manager = Manager(app)
callback = IBWrapper()
client=IBclient(callback, 100)

def _make_context():
	return {'models': models, 'client': client}

@manager.command
def hello():
	print "hello"
	
@manager.command
def init_db():
	Base.metadata.drop_all(engine)
	Base.metadata.create_all(engine)
	
	# import pdb; pdb.set_trace()
	Future.insert_contracts(client, "ES", "GLOBEX")


	s1 = S1()
	s1.name = 'S1'
	s1.description = 'equity index opening range reversion'
	s1.start_time = datetime.time(9,30)
	s1.end_time = datetime.time(10,15)
	s1.interval = datetime.timedelta(minutes=30)
	s1.tz = "US/Eastern"
	s1.scalar = 1
	s1.continuous = False
	s1.save()
	
	s2 = S2()
	s2.name = 'S2'
	s2.description = 'equity index opening range trend'
	s2.start_time = datetime.time(9,30)
	s2.end_time = datetime.time(10,15)
	s2.interval = datetime.timedelta(minutes=30)
	s2.tz = "US/Eastern"
	s2.scalar = 1
	s2.continuous = False
	s2.save()

	s3 = S3()
	s3.name = 'S3'
	s3.description = 'e-mini eod reversion/hedging arb'
	s3.start_time = datetime.time(16,00)
	s3.tz = "US/Eastern"
	s3.scalar = 1
	s3.continuous = False
	s3.save()

@manager.command
def load_data():
	# '''

	series = Series.filter(Series.id==25).first()
	series.insert_history(client, 2000)
	series.update_atr(client)
	# series.update_daily(client)
	#

	import pdb; pdb.set_trace()
	# intraday = Intraday()
	# intraday.load(3, ticker='ES')
	# import pdb; pdb.set_trace()
	# intraday.load(4, ticker='VX')
	# intraday.load(5, ticker='FVS')
	# intraday.load(6, ticker='EX')
	# intraday.load(10, ticker='TY')
	# intraday.load(11, ticker='BD')
	# intraday.load(12, ticker='FV')
	# intraday.load(13, ticker='US')
	# intraday.load(14, ticker='ED')
	# intraday.load(15, ticker='BL')
	# intraday.load(20, ticker='W')
	# intraday.load(21, ticker='CL')
	# intraday.load(22, ticker='HG')
	# intraday.load(23, ticker='GC')
	# intraday.load(24, ticker='C')
	# intraday.load(25, ticker='NG')
	# intraday.load(26, ticker='LE')
	# intraday.load(27, ticker='S')
	# intraday.load(29, ticker='PA')
	# intraday.load(40, ticker='EU')
	# intraday.load(41, ticker='BP')
	# intraday.load(42, ticker='AD')
	# intraday.load(43, ticker='JY')
	# intraday.load(44, ticker='CD')
	# intraday.load(45, ticker='NE')
	# intraday.load(46, ticker='SF')
	# intraday.load(28, ticker='PL')




	print 'loaded'

	import pdb; pdb.set_trace()
	'''
	intraday = Intraday()
	intraday.load(3, ticker='ES')
	intraday.load(4, ticker='VX')
	intraday.load(5, ticker='FVS')
	intraday.load(6, ticker='EX')

	intraday.load(17, ticker='CL')
	intraday.load(25, ticker='NG')

	intraday.load(27, ticker='GC')
	# intraday.load(29, ticker='JY')
	# intraday.load(17, ticker='CL')
	# intraday.load(25, ticker='NG')
	# intraday.load(4, ticker='VX')
	# '''
@manager.command
def backtest():
	# es = Series.filter(Series.id==3).first()
	# es.update(client)
	# s1 = Strategy.filter(Strategy.id==1).first()
	# import pdb; pdb.set_trace()
	# s1_bt = s1.backtest(datetime.date(2015,1,1), intraday=True)

	# test = Intraday.pull_minute_bars(3, datetime.time(15, 59))
	# import pdb; pdb.set_trace()
	# s4 = Strategy.filter(Strategy.id==7).first()
	# import pdb; pdb.set_trace()
	# s4_bt = s4.backtest(datetime.date(2010,1,1), intraday=True, slippage=True)

	# import pdb; pdb.set_trace()
	s4 = Strategy.filter(Strategy.id==7).first()
	# import pdb; pdb.set_trace()
	s4_bt = s4.backtest(datetime.date(2015,1,1), intraday=True, slippage=True)
	import pdb; pdb.set_trace()
	# s6 = Strategy.filter(Strategy.id==6).first()
	# s6_bt = s6.backtest(datetime.date(2010,1,1), intraday=True)

	import pdb; pdb.set_trace()

@manager.command
def spread_test():
	from ibtrader import IBTrader

	ibtrader = IBTrader(client)
	ibtrader.reconcile_spreads()


	vx = Series.filter(Series.id == 3).first()

	vx1 = vx.contract(1)
	vx2 = vx.contract(2)
	spd = Spread(vx1, vx2)
	ibcontract = spd.contract(client)
	# strategy = Strategy.filter(Strategy.id==8).first()
	# strategy.live = True
	# spd.place_order2(1, strategy, "MKT", target=True)

	order = Order.filter(Order.id==4170).first()
	market_data1 = client.get_IB_market_data(vx1.contract, seconds=1.0)
	market_data2 = client.get_IB_market_data(vx2.contract, seconds=1.0)

	orderid = client.place_new_IB_order(ibcontract=ibcontract, trade=order.trade_amt, px=order.trade_px, orderid=952457, orderType=str(order.type), startTime=order.starttime, endTime = order.endtime)



@manager.command
def carry_bt():
	curve = 7
	ar_window = 250

	series = Series.filter(Series.id==14).first()
	contracts = Contract.filter_df(Contract.seriesid==series.id).rename(columns={'id': 'contractid'})[['contractid', 'expiry_dt']]

	out = pandas.DataFrame()
	for i in xrange(1,curve):
		dat = series.data(start_date=datetime.datetime(2000,1,1), contract_num=i).merge(contracts, on='contractid')
		dat['dte'] = (dat.expiry_dt - dat.dt) / numpy.timedelta64(1, 'D')

		dat = dat[['dt', 'close_', 'chg', 'dte', 'atr10']]
		dat.columns = ['dt'] + ['{0}{1}'.format(x, i) for x in ['close_', 'chg', 'dte', 'atr10']]

		if len(out) == 0:
			out = dat
		else:
			out = out.merge(dat, on='dt', how='left')
			out['carry{0}'.format(i)] = (out['close_{0}'.format(i)] - out['close_1']) / (out['dte{0}'.format(i)] - out['dte1'])
			out['carry{0}'.format(i)] = out['carry{0}'.format(i)].fillna(out['carry{0}'.format(i)].shift(1))
	import pdb; pdb.set_trace()
	close = out[['dt'] + ['close_{0}'.format(i) for i in xrange(1,curve)]]
	dte = out[['dt'] + ['dte{0}'.format(i) for i in xrange(1,curve)]]
	ret_a = out[['dt'] + ['chg{0}'.format(i) for i in xrange(1,curve)]].fillna(0)
	carry = out[['dt'] + ['carry{0}'.format(i) for i in xrange(2,curve)]]
	ret = out[['dt'] + ['chg{0}'.format(i) for i in xrange(2,curve)]].fillna(0)
	atr = out[['dt'] + ['atr10{0}'.format(i) for i in xrange(2,curve)]].fillna(0)

	# positions = deepcopy(carry)
	# carry_max = carry.max(1)
	# carry_min = carry.min(1)
	# for col in carry.columns:
	# 	positions[col] = (positions[col] == carry_min).astype('int') - (positions[col] == carry_max).astype('int')

	carry_resid = pandas.DataFrame()
	for i in xrange(ar_window, len(carry)):
		print i
		ar_dat = carry.iloc[0: i]

		residuals = []
		for col in ar_dat.drop('dt', 1).columns:
			int_values = ar_dat[col].dropna().values
			robjects.r.assign('.temp', numpy2ri(int_values))
			# robjects.r('fit = auto.arima(.temp)')
			# resid = robjects.r('fit$residuals[length(fit$residuals)]')[0]

			robjects.r('fit = ar(.temp, order=1)')
			resid = robjects.r('fit$resid[length(fit$resid)]')[0]

			# import pdb; pdb.set_trace()
			#
			# model = AR()
			# fitted = model.fit(maxlag=1)
			# resid = ar_dat[col].iloc[-1] - fitted.fittedvalues[-1]
			residuals.append(resid)

		carry_resid = carry_resid.append(pandas.DataFrame([[ar_dat.iloc[-1]['dt']] + residuals]))

	positions = deepcopy(carry_resid)
	carry_max = carry_resid.max(1)
	carry_min = carry_resid.min(1)
	for col in carry_resid.drop(0, 1).columns:
		positions[col] = (positions[col] == carry_min).astype('int') - (positions[col] == carry_max).astype('int')


	ret_cut = ret[ret.dt.isin(carry_resid[0])]
	atr_cut = atr[atr.dt.isin(carry_resid[0])]
	import pdb; pdb.set_trace()
	pandas.DataFrame(positions.drop(0,1).shift(1).values * ret_cut.drop('dt',1).values).sum(1).cumsum().plot()
	pandas.DataFrame((positions.drop(0,1).shift(1).values * ret_cut.drop('dt',1).values).sum(1)).to_csv('b1.csv')
if __name__ == "__main__":
	
	manager.add_command('shell', Shell(make_context=_make_context))
	manager.run()