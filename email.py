import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import formatdate

sender = 'imchugh@ozflux-grunt.novalocal'
receivers = ['ian_mchugh@fastmail.com']

def email_test(to, subject, bodytext):

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
    except SMTPException:
        print "Error: unable to send email"

    smtpObj.close()

bodytext = 'Hello World'
subject = 'The dead eyes opened'
email_test(receivers, subject, bodytext)
