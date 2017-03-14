from tzlocal import get_localzone
import datetime
import pytz
from sqlalchemy import Table
import db.models as models

def fut_date(dt):
	try:
		if dt.time() >= datetime.time(18, 00):
			return dt.date() + datetime.timedelta(days=1)
		else:
			return dt.date()
	except:
		return dt

def convert_ib_tz(ib_tz):
	if ib_tz == 'GMT':
		return 'Etc/GMT'
	elif ib_tz == 'EST':
		return 'US/Eastern'
	elif ib_tz == 'CST':
		return 'US/Central'
	elif ib_tz == 'MST':
		return 'US/Mountain'
	elif ib_tz == 'PST':
		return 'US/Pacific'
	elif ib_tz == 'AST':
		return 'US/Eastern'
	elif ib_tz == 'JST':
		return 'Japan'
	elif ib_tz == 'AET':
		return 'Australia/Canberra'
	else:
		print 'Unsupported Timezone'
		return None

def convert_local(dt, tz_string, today=None):
	local_tz = get_localzone()

	#convert time to datetime
	if type(dt) is datetime.time:
		if not today:
			today = datetime.datetime.now().date()
		dt = datetime.datetime.combine(today, dt)

	dt_tz = pytz.timezone(tz_string).localize(dt)
	dt_local = dt_tz.astimezone(local_tz)

	return dt_local


def convert_EST(dt, tz_string):
	local_tz = pytz.timezone('US/Eastern')

	#convert time to datetime
	if type(dt) is datetime.time:
		today = datetime.datetime.now().date()
		dt = datetime.datetime.combine(today, dt)

	dt_tz = pytz.timezone(tz_string).localize(dt)
	dt_local = dt_tz.astimezone(local_tz)

	return dt_local

def clone_table(name, table, metadata):
	cols = [c.copy() for c in table.columns]
	constraints = [c.copy() for c in table.constraints]
	return Table(name, metadata, *(cols + constraints), prefixes=['TEMPORARY'])

def check_data_pipelines(client):
	es = models.trading.Series.filter(models.trading.Series.id==3).first()

	#checking
	es.front_contract.market_data(client)['last']
	es.front_contract.hist_data(client).iloc[0]

	return True
