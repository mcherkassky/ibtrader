import time
import datetime
from db.models.trading import Order, Fill, Contract, Series
from db.models.data import Heartbeat
from ibclient.models import IBWrapper, IBclient
from sqlalchemy.sql import func
from sqlalchemy import and_
from db.sqlite import *
import traceback
import pandas
from utils.mail import Email
from utils import check_data_pipelines
from utils.config import EMAIL_ALL, EMAIL_CRITICAL
from sklearn.cluster import KMeans

callback = IBWrapper()
client = IBclient(callback, 102)

class IBTrader:
	def __init__(self, client):
		self.client = client

	def check_integrity(self):
		
		check_orders = self.check_orders()
		check_positions = self.check_positions()

		if all([check_orders, check_positions]):
			return True
		else:
			return False

	def check_orders(self):
		return True

	def check_positions(self):
		db_positions = Fill.current_positions().rename(columns={'trade_amt': 'db'})
		broker_positions = self.get_broker_positions().rename(columns={'trade_amt': 'broker'})

		if len(broker_positions) == 0:
			broker_positions = pandas.DataFrame(columns=['contractid', 'broker'])
		if len(db_positions) == 0:
			db_positions = pandas.DataFrame(columns=['contractid', 'db'])

		check_df = db_positions.merge(broker_positions, on='contractid', how='outer').fillna(0)

		if all(check_df.db == check_df.broker):
			return True

		else: #positions don't match
			fills = self.broker_fills()

			email = Email("CRITICAL: {0} DB Broker Positions Don't Match".format(datetime.datetime.now()),
							'{0} <br> {1}'.format(check_df.to_html(index=False), fills.to_html(index=False)))
			email.send(EMAIL_CRITICAL)

			self.reconcile()

			return False

	def reconcile_spreads(self):
		todays_fills = session.query(Fill).filter(func.date(Fill.dt)== datetime.datetime.now().date()).subquery()
		query = session.query(todays_fills, Series.type).join(Contract, and_(todays_fills.c.contractid == Contract.id)).join(Series, and_(Contract.seriesid == Series.id))
		fills_df = pandas.read_sql(query.statement, query.session.bind)

		spread_fills = fills_df[fills_df.type == 'SPD']
		for orderid in spread_fills.orderid.unique():
			order_fills = spread_fills[spread_fills.orderid == orderid]

			#fills need reconciliation
			if len(order_fills) % 3 == 0:
				#first remove spread fills
				kmeans = KMeans(n_clusters=2, random_state=0).fit(numpy.array([[a] for a in order_fills.trade_px.values]))
				order_fills['labels'] = kmeans.labels_

				if sum(order_fills['labels'])*1.0 / len(order_fills['labels']) > 0.5:
					leg_fills = order_fills[order_fills['labels'] == 1]
				else:
					leg_fills = order_fills[order_fills['labels'] == 0]

				#next consolidate individual legs
				long_orders = leg_fills[leg_fills.trade_amt > 0]
				short_orders = leg_fills[leg_fills.trade_amt < 0]

				final_long_id = long_orders[long_orders.dt == long_orders.dt.max()].id.values[0]
				final_short_id = short_orders[short_orders.dt == short_orders.dt.max()].id.values[0]

				#delete other fills
				ids_to_delete = order_fills[~order_fills.id.isin([final_long_id, final_short_id])].id.astype('int').values


				#update remaining fills
				spd = Contract.filter(Contract.id == int(order_fills.contractid.iloc[0])).first().spread
				long_fill = Fill.filter(Fill.id == int(final_long_id))
				short_fill = Fill.filter(Fill.id == int(final_short_id))

				long_fill.update({'contractid': spd.long_leg.id})
				short_fill.update({'contractid': spd.short_leg.id})
				Fill.filter(Fill.id.in_(ids_to_delete)).delete(synchronize_session='fetch')
				import pdb; pdb.set_trace()
				session.commit()

			else:
				continue

	def reconcile(self):
		broker_fills = self.broker_fills()

		# db_fills = Fill.filter_df(func.date(Fill.dt) == datetime.datetime.today().date())
		query = session.query(Fill, Order.ib_id).join(Order).filter(func.date(Fill.dt) == datetime.datetime.today().date())
		db_fills = pandas.read_sql(query.statement, query.session.bind)
		broker_fills.ib_id = broker_fills.ib_id.astype('int64')

		comparison = broker_fills.merge(db_fills.drop('dt', 1), on=['contractid', 'ib_id', 'trade_amt', 'trade_px'], how='left')
		unaccounted_fills = comparison[comparison.orderid.isnull()]

		for i in xrange(len(unaccounted_fills)):

			fill = unaccounted_fills.iloc[i]
			order = Order.filter(and_(Order.ib_id == int(fill.ib_id), func.date(Order.dt) == fill['dt'].date())).first()

			if order:
				unaccounted_fills.ix[fill.name,'orderid'] = order.id
			else:
				email = Email("CRITICAL: {0} Fill Reconciliation Failed".format(datetime.datetime.now()),"")
				email.send(EMAIL_CRITICAL)
				return False

		upload = unaccounted_fills.drop(['cusip','id','ib_id'],1)
		upload_dict = upload.to_dict(orient='records')

		Fill.__table__.insert().execute(upload_dict)

		email = Email("CRITICAL: {0} Reconciliation Success".format(datetime.datetime.now()),"")
		email.send(EMAIL_CRITICAL)
		return True


	def check_fills(self):
		pass

	def broker_fills(self):
		fills = client.get_executions()
		fills_df = pandas.DataFrame()
		for k, fill in fills.iteritems():
			trade_px = fill['price']
			trade_amt = fill['qty'] * (-1 if fill['side'] == 'SLD' else 1)
			ib_id = k
			dt = datetime.datetime.strptime(fill['times'], '%Y%m%d  %H:%M:%S')

			ticker = fill['symbol']
			expiry_dt = datetime.datetime.strptime(fill['expiry'], '%Y%m%d')

			series = Series.filter(Series.symbol == ticker, Series.type <> 'IND').first()
			contract = Contract.filter(and_(Contract.seriesid == series.id, Contract.expiry_dt == expiry_dt)).first()

			fills_df = fills_df.append(pandas.DataFrame({'dt': [dt], 'contractid':[contract.id], 'ib_id':[ib_id], 'cusip':[contract.cusip], 'trade_amt':[trade_amt], 'trade_px':[trade_px]}))

		return fills_df


	def get_broker_positions(self):
		acct_value, positions = client.get_IB_account_data()

		positions_df = pandas.DataFrame()

		#positions to contractids
		for position in positions:
			ticker = position[0]
			expiry_dt = datetime.datetime.strptime(position[1], '%Y%m%d')
			trade_amt = int(position[2])

			series = Series.filter(Series.symbol == ticker, Series.type == 'FUT').first()
			contract = Contract.filter(and_(Contract.seriesid == series.id, Contract.expiry_dt == expiry_dt)).first()

			try:
				positions_df = positions_df.append(pandas.DataFrame({'contractid' :[contract.id], 'trade_amt':[trade_amt]}))
			except AttributeError:
				print 'Unable to find Contract {0} with expiry {1}'.format(series.name, expiry_dt)

				front = series.front_contract
				print 'Updating ContractID {0} (Front Contract) Expiry to {1}'.format(front.id, expiry_dt)

				front.expiry_dt = expiry_dt
				front.save()

				raise Exception('Flow Exception: Restarting IBTrader')


		return positions_df

	def pending_orders(self):

		now = datetime.datetime.now()
		
		#orders = Order.filter(Order.dt>=now.date(), Order.ib_id==None, Order.starttime<=now, Order.endtime>=now).all()
		orders = Order.filter(Order.ib_id==None, Order.starttime<=now, Order.endtime>=now).all()

		return orders
		
	def send_to_ib(self, orders):
		market_data_cache = {}

		for order in orders:
			contract = Contract.filter(Contract.id==order.contractid).first()
			series = contract.series

			if series.type == 'SPD':
				spread = contract.spread
				ibcontract = spread.contract(client)
			else:
				ibcontract=contract.contract

			if order.target: #target based
				current_position = session.query(Fill, func.sum(Fill.trade_amt)) \
											.join(Order).\
											filter(and_(Fill.contractid==order.contractid, Order.strategyid==order.strategyid)).first()[1]

				if not current_position:
					current_position = 0

				trade_amt = order.trade_amt - current_position
			else:
				trade_amt = order.trade_amt


			orderid = None

			if trade_amt <> 0:
				#custom stop order
				if order.type == 'STOPC':
					if contract.id not in market_data_cache:
						mkt = contract.market_data(client)
						market_data_cache[contract.id] = mkt['last']

					mkt_px = market_data_cache[contract.id]

					if (trade_amt > 0 and mkt_px >= order.trade_px) or (trade_amt < 0 and mkt_px <= order.trade_px):
						orderid = self.client.place_new_IB_order(ibcontract=ibcontract,
																 trade=trade_amt,
																 px=0.0,
																 orderType=str('MKT'),
																 startTime=order.starttime,
																 endTime=order.endtime)
					else:
						pass

				else:
					orderid = self.client.place_new_IB_order(ibcontract=ibcontract,
													 trade=trade_amt,
													 px=order.trade_px,
													 orderType=str(order.type),
													 startTime=order.starttime,
													 endTime = order.endtime)

			if orderid:
				order.ib_id = orderid
				order.save()

			
	def run(self):
		i = 0
		last_error = None

		while True:
			try:
				if self.check_integrity():
					pending = self.pending_orders()

					if pending:
						self.send_to_ib(pending)

					now = datetime.datetime.now()
					if now.time() > datetime.time(14,05) and now.time() < datetime.time(14,15):
						break

					if i%6 == 0:
						print 'Heartbeat: {0}'.format(now)



					i +=1
					last_error = None

					# #update heartbeat table
					Heartbeat.update('Trader')

					time.sleep(10)

				else:
					print "{0} DB Integrity Check Failed: Sleeping for 30 seconds".format(datetime.datetime.now())
					i = 0
					time.sleep(30)

			except Exception as e:
				error_str = traceback.format_exc()

				if not (type(last_error) == type(e) and last_error.args == e.args):
					email = Email("{0} IBTrader Bugging Out".format(datetime.datetime.now()), error_str)
					email.send(EMAIL_CRITICAL)

					#restart/rollback db session
					session.rollback()

				last_error = e



			
if __name__ == '__main__':
	ibtrader = IBTrader(client)

	ibtrader.run()