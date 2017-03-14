import schedule
import datetime
import pytz
import time
from ibclient.models import IBWrapper, IBclient
from db.models.trading import *
from db.models.data import *
from tzlocal import get_localzone
from utils.mail import *
import matplotlib.pyplot as plt

from strategy import *
from utils import check_data_pipelines
from utils.config import EMAIL_ALL, EMAIL_CRITICAL, EMAIL_TEXT


local_tz = get_localzone()
callback = IBWrapper()
client = IBclient(callback, 101)


if __name__ == '__main__':
	strategies = Strategy.filter(Strategy.paper == 0).all()
	today = datetime.datetime.now().date()

	for strategy in strategies:
		#strategy has universe table
		if strategy.universe:
			Universe = eval(strategy.universe)
			for series_info in Universe.filter(Universe.id > 0).all():

				start_time_tz = datetime.datetime.combine(today, series_info.starttime)
				start_time_tz = pytz.timezone(series_info.tz).localize(start_time_tz)

				start_time_local = start_time_tz.astimezone(local_tz)
				#
				# if strategy.name == 'S4':
				# 	if series_info.__dict__['eq_series'] == 3:
				# 		strategy.run(**series_info.__dict__)

				schedule.every().day.at(start_time_local.strftime("%H:%M")).do(strategy.run, **series_info.__dict__)

		else:
			start_time_tz = datetime.datetime.combine(today, strategy.start_time)
			start_time_tz = pytz.timezone(strategy.tz).localize(start_time_tz)

			start_time_local = start_time_tz.astimezone(local_tz)

			schedule.every().day.at(start_time_local.strftime("%H:%M")).do(strategy.run)

		#run summaries
		schedule.every().day.at('13:15').do(strategy.summary)


	i = 0
	last_error = None
	while True:
		try:
			schedule.run_pending()

			now = datetime.datetime.now()
			if now.time() > datetime.time(14,05) and now.time() < datetime.time(14,15):
				break

			if i%60 == 0:
				print "OMS Heartbeat: {0}".format(datetime.datetime.now())

			i +=1
			time.sleep(1)

			#update heartbeat table
			if i%15 == 0:
				Heartbeat.update('OMS')


		except Exception as e:
			error_str = traceback.format_exc()

			if not (type(last_error) == type(e) and last_error.args == e.args):
				email = Email("{0} OMS Bugging Out".format(datetime.datetime.now()), error_str)
				email.send(EMAIL_CRITICAL)

				#restart/rollback db session
				session.rollback()

			last_error = e
	