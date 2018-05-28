import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import formatdate

def email_send(to, subject, bodytext):

    assert isinstance(to, list)
    to_str = ','.join(to)
    fro = 'noreply@ozflux-grunt.novalocal'

    msg = MIMEMultipart()
    msg['From'] = fro
    msg['To'] = to_str
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = subject
    msg.attach(MIMEText(bodytext))

    try:
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail(fro, to, msg.as_string())        
        print "Successfully sent email"
    except Exception, e:
        print "Error: unable to send email"
        return

    smtpObj.close()
