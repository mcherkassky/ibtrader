import time
import datetime
from strategy import *
from db.models.trading import *
from db.models.data import *
from ibclient.models import IBWrapper, IBclient
from pandas.tseries.offsets import *
from sqlalchemy.sql import func, and_, or_
from db.sqlite import *
from time import sleep
from utils.config import DB_PATH, DB_NAME
from shutil import copyfile
import os
import matplotlib.pyplot as plt
from utils.config import EMAIL_ALL, EMAIL_CRITICAL, EMAIL_TEXT

callback = IBWrapper()
client=IBclient(callback, 27)

class DataUpdater:
	def __init__(self, client):
		self.client = client

	def update_daily(self):
		active_series = Series.filter(Series.active==True)

		for series in active_series:
			print 'Updating {0}'.format(series.name)
			series.update(client)
			sleep(20)

	def db_backup(self):
		copyfile(os.path.join(DB_PATH, DB_NAME), os.path.join(DB_PATH, DB_NAME + '.backup'))

	def run(self):
		today = datetime.datetime.now()
		self.update_daily()
		self.db_backup()

		#reporting
		if (fut_date(today) - today.date()).days > 0:
			self.slippage_report()
		else:
			self.daily_summary()

		'''
		#one off update vx
		for seriesid in [4,5]:
			today = datetime.datetime.now().replace(hour=8, minute=30, second=0, microsecond=0)
			today_est = datetime.datetime.now().replace(hour=11, minute=30, second=0, microsecond=0)
			vx = Series.filter(Series.id == seriesid).first()
			data = vx.front_contract.hist_data(client, durationStr="60 S", barSizeSetting="1 min", endDateTime=today)
			data['dt'] = today_est
			data['contractid'] = vx.front_contract.id
			data = data.rename(columns={'open':'open_', 'close':'close_'})
			dat_dict = data.to_dict(orient='records')
			Intraday.__table__.insert().execute(dat_dict)
		'''

	def slippage_report(self, trade_start=datetime.date(2016, 1, 1)):
		#run ytd backtests
		strategies = Strategy.filter(or_(Strategy.name == 'S7', Strategy.name=='S8', Strategy.name == 'S3', Strategy.paper == 0))
		ytd = datetime.datetime.now().replace(month=1, day=1).date()
		start_dt = max(ytd, trade_start)

		bt_pnl = pandas.DataFrame()
		rt_pnl = pandas.DataFrame()

		strategy_plots = []
		email_dummy = Email("","")

		for strategy in strategies:
			bt = strategy.backtest(bt_start=ytd, intraday=strategy.intraday, slippage=True)

			if strategy.paper == 0:
				bt_pnl = bt_pnl.append(bt)

			bt_strategy_pnl = bt[bt.dt >= start_dt].pivot('dt', 'seriesid').fillna(0).sum(1)
			rt_strategy_pnl = strategy.pnls

			rt_pnl = rt_pnl.append(rt_strategy_pnl)

			if len(rt_strategy_pnl) > 0:
				rt_strategy_pnl = rt_strategy_pnl[rt_strategy_pnl.dt >= start_dt].groupby('dt').pnl.sum()
			else:
				rt_strategy_pnl = pandas.DataFrame({}, columns=['pnl'])

			slippage = pandas.DataFrame(bt_strategy_pnl).join(pandas.DataFrame(rt_strategy_pnl), how='outer').fillna(0).cumsum()
			slippage.columns = ['bt_pnl', 'rt_pnl']
			# slippage.rt_pnl = 0
			ax = slippage.plot().get_figure()

			plt.title('Strategy {0} RT vs. BT'.format(strategy.name))
			strategy_plots.append(ax)

		bt_pnl.index = range(len(bt_pnl))
		bt_all = pandas.DataFrame(bt_pnl.groupby('dt').sum().pnl)
		rt_all = pandas.DataFrame(rt_pnl.groupby('dt').sum().pnl)

		bt_all['dt'] = pandas.to_datetime(bt_all.index)
		rt_all['dt'] = pandas.to_datetime(rt_all.index)
		all_slippage = bt_all.merge(rt_all, on='dt').rename(columns={'pnl_x': 'bt_pnl', 'pnl_y': 'rt_pnl'})
		# all_slippage['rt_pnl'] = 0
		slippage_plt = all_slippage[all_slippage.dt >= start_dt]
		slippage_plt.index = slippage_plt.dt
		slippage_plt = slippage_plt.drop('dt', 1)

		correlation = round(numpy.corrcoef(slippage_plt.rt_pnl, slippage_plt.bt_pnl)[0][1], 2)

		email = Email("{0} Slippage Report, Live - BT: {1}".format(datetime.datetime.now().date(), round(slippage_plt.rt_pnl.sum() - slippage_plt.bt_pnl.sum(),2)), "")
		ax = slippage_plt.cumsum().plot().get_figure()
		plt.title('Aggregate Live vs Backtest PnL, Correlation={0}'.format(correlation))
		email.add_figure(ax, imgid=0)

		plt.clf()

		ax = bt_all.pnl.cumsum().plot().get_figure()
		plt.title('Aggregate YTD Backtested PnL')
		plt.axvline(datetime.datetime(2016,5,1),color='r', linestyle='--', lw=2)
		email.add_figure(ax, imgid=1)



		for i, ax in enumerate(strategy_plots):
			email.add_figure(ax, imgid=i+2)

		email.send(EMAIL_ALL)

	def daily_summary(self):
		strategies = Strategy.filter(Strategy.paper == 0).all()
		today = datetime.datetime.now().date()

		positions = pandas.DataFrame()
		fills = pandas.DataFrame()
		pnls = pandas.DataFrame()

		for strategy in strategies:
			positions = positions.append(strategy.positions)
			strategy_fills = strategy.fills
			strategy_fills = strategy_fills[strategy_fills.fut_date.map(lambda x: x == today)]
			fills = fills.append(strategy_fills)

			fill_pnls = strategy.pnls


			if len(fill_pnls):
				fill_pnls['dt'] = fill_pnls.dt.map(lambda x: x.date())
				fill_pnls['total_pnl'] = fill_pnls.trading_pnl.fillna(0) + fill_pnls.widening_pnl.fillna(0)
				fill_pnls['strategy'] = strategy.name

				pnls = pnls.append(fill_pnls[['dt', 'strategy', 'cusip', 'trading_pnl', 'widening_pnl', 'total_pnl']])

		#create plot
		plt.clf()
		pnls = pnls[pnls.dt <= fut_date(datetime.datetime.now())]
		pnls_agg = pnls.groupby('dt').total_pnl.sum().sort_index().asfreq(BDay()).fillna(0)
		sharpe = pnls_agg.mean() / pnls_agg.std() * numpy.sqrt(251)

		ax = pnls_agg.cumsum().plot()
		plt.legend(numpoints=1, loc=2)
		plt.title('Historical PnL, Total PnL=${0}, Sharpe={1}'.format(round(pnls_agg.sum(),2), round(sharpe,2)))
		# import pdb; pdb.set_trace()
		pnls = pnls[pnls.dt==today]

		html = ""
		html += "<h3>Current Positions</h3>" + positions[positions.trade_amt != 0].to_html(index=False)
		html += "<h3>Today's Fills</h3>" + fills.to_html(index=False)
		html += "<h3>PnL Breakdown</h3>" + pnls[pnls.total_pnl != 0].to_html(index=False)

		pnl_hist = pnls.groupby('dt').total_pnl.sum()

		email = Email("{0} Daily Summary, Total PnL: {1}".format(today, pnls.total_pnl.sum()), html)
		email.add_figure(plt)
		email.send(EMAIL_ALL)



if __name__ == '__main__':
	updater = DataUpdater(client)

	updater.run()