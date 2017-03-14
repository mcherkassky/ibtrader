import time
import datetime
from db.models.data import Heartbeat

class Monitor:
	def __init__(self):
		pass

	def check(self):
		Heartbeat.check()

	def run(self):
		i = 0
		while True:
			now = datetime.datetime.now()
			print 'Monitor Heartbeat: {0}'.format(now)
			self.check()
			if now.time() > datetime.time(14,05) and now.time() < datetime.time(14,15):
				break

			time.sleep(60)

if __name__ == '__main__':
	monitor = Monitor()

	monitor.run()