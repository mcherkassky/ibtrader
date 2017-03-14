import smtplib
import io
from email import Encoders
from email.mime.multipart import MIMEMultipart, MIMEBase
from email.mime.text import MIMEText
from config import *

class Email:
	def __init__(self, subject, body):
		self.fromaddr = 'mcherkassky@gmail.com'
		self.body = body
		msg = MIMEMultipart('alternative')
		msg['Subject'] = subject
		msg['From'] = self.fromaddr
		
		self.msg = msg
	
	def add_figure(self, plt, imgid=1):
		buf = io.BytesIO()
		plt.savefig(buf, format = 'png')
		buf.seek(0)

		msgText = '<br><img src="cid:image{0}"><br>'.format(imgid)
		self.body = self.body + msgText

		part = MIMEBase('application', "octet-stream")
		part.set_payload( buf.read() )
		Encoders.encode_base64(part)
		part.add_header('Content-Disposition', 'attachment; filename="%s"' % 'figure.png')
		part.add_header('Content-ID', '<image{0}>'.format(imgid))
		self.msg.attach(part)

		buf.close() #close buffer

	def send(self, toaddr):
		try:
			server = smtplib.SMTP('smtp.gmail.com:587', timeout=10)
			server.starttls()
			server.login(GMAIL_USER, GMAIL_PW)

			self.msg['To'] = ', '.join(toaddr)
			self.msg.attach(MIMEText(self.body, 'html'))
			server.sendmail(self.fromaddr, toaddr, self.msg.as_string())
			server.quit()
		except:
			print 'Email Error'
			return None
		