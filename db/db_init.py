from db.models import Future
from db.sqlite import *
from strategy.s1 import S1
from strategy.s2 import S2
from strategy.s3 import S3

if __name__ == "__main__":
	Base.metadata.create_all(engine)
	
	# import pdb; pdb.set_trace()

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
	
	import pdb; pdb.set_trace()