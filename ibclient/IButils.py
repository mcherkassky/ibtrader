import pandas as pd
import numpy as np
from db.models.trading import Order, Fill, Strategy, Contract, Series
from db.sqlite import engine, DBSession

DEFAULT_VALUE=np.nan

class autodf(object):
	'''
	Object to make it easy to add data in rows and return pandas time series
	
	Initialise with autodf("name1", "name2", ...)
	Add rows with autodf.add_row(name1=..., name2=...., )
	To data frame with autodf.to_pandas
	'''

	def __init__(self, *args):
		
		
		storage=dict()
		self.keynames=args
		for keyname in self.keynames:
			storage[keyname]=[]
			
		self.storage=storage 
		
	def add_row(self, **kwargs):
		
		for keyname in self.storage.keys():
			if keyname in kwargs:
				self.storage[keyname].append(kwargs[keyname])
			else:
				self.storage[keyname].append(DEFAULT_VALUE)

	def to_pandas(self, indexname=None):
		if indexname is not None:
			data=self.storage

			index=self.storage[indexname]
			data.pop(indexname)
			return pd.DataFrame(data, index=index)
		else:
			return pd.DataFrame(self.storage)
		
def bs_resolve(x):
	if x<0:
		return 'SELL'
	if x>0:
		return 'BUY'
	if x==0:
		raise Exception("trying to trade with zero")
		
def action_ib_fill(execlist):
	"""
	Get fills (either ones that have just happened, or when asking for orders)
	
	Note that fills are cumulative, eg for an order of +10 first fill would be +3, then +9, then +10
	implying we got 3,6,1 lots in consecutive fills
	
	The price of each fill then is the average price for the order so far 
	"""

	print(execlist)

	orderid = execlist['orderid']
	local_session = DBSession()

	order = local_session.query(Order).filter(Order.ib_id==orderid).order_by(Order.dt.desc()).first()
	contract = local_session.query(Contract).filter(Contract.id == order.contractid).first()
	series = contract.series

	if execlist['side'] == 'BOT':
		trade_amt=execlist['qty']
	else:
		trade_amt=execlist['qty'] * -1.0

	if series.type == 'SPD':
		fill = Fill(contractid=order.contractid, orderid=order.id, trade_amt=trade_amt, trade_px=execlist['price'])
		local_session.add(fill)
		local_session.commit()
	else:
		existing_fill = local_session.query(Fill).filter(Fill.orderid==order.id)
		if existing_fill.first() is not None:
			existing_fill.update({'trade_amt': trade_amt, 'trade_px': execlist['price']})
			local_session.commit()
		else:
			fill = Fill(contractid=order.contractid, orderid=order.id, trade_amt=trade_amt, trade_px=execlist['price'])

			#old code
			# order.save()
			# fill.save()


			# local_session.add(order)
			local_session.add(fill)
			local_session.commit()

	#callback function
	if order.callback:
		#somewhat of a hack, unsure of how to fix
		res = engine.execute('select type from Strategy where id={0}'.format(order.strategyid))
		discriminator = [r for r in res][0][0]
		exec("from strategy import {0}".format(discriminator))
		strategy = Strategy.filter(Strategy.id == order.strategyid).first()

		callback_func = getattr(strategy, order.callback)

		callback_func(order)

		
