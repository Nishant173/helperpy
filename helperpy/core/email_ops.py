from typing import List, Optional
from email import encoders
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import mimetypes
import smtplib

from helperpy.core.file_io import get_basename_from_filepath


def __add_attachments_to_multipart_object(
        filepaths_to_attachments: List[str],
        multipart_obj: MIMEMultipart,
    ) -> MIMEMultipart:
    """
    Adds attachments from the given filepaths to the given `MIMEMultipart` object.
    Returns `MIMEMultipart` object containing the attachments.

    Resources:
        - [Python email package examples](https://www.rose-hulman.edu/class/cs/archive/csse120-old/csse120-old-terms/201210/Resources/python-3.1.2-docs-html/library/email-examples.html)
        - [Common MIME types](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types)
    """
    for filepath in filepaths_to_attachments:
        type_, encoding = mimetypes.guess_type(url=filepath)
        if type_ is None or encoding is not None:
            type_ = 'application/octet-stream'
        maintype, subtype = type_.split(sep='/', maxsplit=1)
        if maintype == 'text':
            fp = open(file=filepath)
            payload = MIMEText(_text=fp.read(), _subtype=subtype)
            fp.close()
        elif maintype == 'image':
            fp = open(file=filepath, mode='rb')
            payload = MIMEImage(_imagedata=fp.read(), _subtype=subtype)
            fp.close()
        elif maintype == 'audio':
            fp = open(file=filepath, mode='rb')
            payload = MIMEAudio(_audiodata=fp.read(), _subtype=subtype)
            fp.close()
        else:
            fp = open(file=filepath, mode='rb')
            payload = MIMEBase(_maintype=maintype, _subtype=subtype)
            payload.set_payload(payload=fp.read())
            fp.close()
            encoders.encode_base64(msg=payload)
        payload.add_header(
            _name='Content-Disposition',
            _value='attachment',
            filename=get_basename_from_filepath(filepath=filepath),
        )
        multipart_obj.attach(payload=payload)
    return multipart_obj


def send_email(
        from_email_id: str,
        from_email_id_password: str,
        to_email_ids: List[str],
        cc_email_ids: List[str],
        subject: str,
        body: str,
        filepaths_to_attachments: Optional[List[str]] = None,
    ) -> None:
    """
    Sends an email from one Email ID to one or more Email IDs, along with the attachments provided (if any).
    Attachments work for the following file extensions: ['csv', 'docx', 'flv', 'jpg', 'm4a', 'mp3', 'mp4', 'pdf', 'png', 'txt', 'xls', 'xlsx', 'zip'].
    Accepts HTML tags for the `body` parameter.
    Returns None if the email is sent successfully; otherwise raises an Exception.

    >>> send_email(
            from_email_id='sender_email_id@gmail.com',
            from_email_id_password='some_password',
            to_email_ids=['person1@gmail.com', 'person2@gmail.com'],
            cc_email_ids=['person3@gmail.com'],
            subject="Your subject",
            body="Your message",
            filepaths_to_attachments=['file1.xlsx', 'file2.pdf'],
        )
    """
    msg = MIMEMultipart()
    msg['From'] = from_email_id
    msg['To'] = ", ".join(to_email_ids)
    msg['Cc'] = ", ".join(cc_email_ids)
    msg['Subject'] = subject
    msg.attach(payload=MIMEText(_text=body, _subtype='html'))
    if filepaths_to_attachments is not None:
        msg = __add_attachments_to_multipart_object(
            filepaths_to_attachments=filepaths_to_attachments,
            multipart_obj=msg,
        )
    server = smtplib.SMTP(host='smtp.gmail.com', port=587)
    server.starttls()
    server.ehlo()
    server.login(user=from_email_id, password=from_email_id_password)
    server.sendmail(
        from_addr=msg['From'],
        to_addrs=msg['To'].split(',') + msg['Cc'].split(','),
        msg=msg.as_string(),
    )
    server.quit()
    return None