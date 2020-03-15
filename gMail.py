from __future__ import print_function
import pickle
import os.path
import base64
from lxml.html import fromstring
from lxml.etree import tostring

from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient import errors

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

def auth():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        service = build('gmail', 'v1', credentials=creds)
        return service

def SendMessage(service, user_id, message):
    """Send an email message.
    
    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      message: Message to be sent.
    
    Returns:
      Sent Message.
    """
    try:
        message = (service.users().messages().send(userId=user_id, body=message).execute())
        #print('Message Id: {}'.format(message['id']))
        return message
    except (errors.HttpError, error):
        print('An error occurred: {}'.format(error))

def CreateMessage(sender, to, subject, message_text):
    """Create a message for an email.
    
    Args:
      sender: Email address of the sender.
      to: Email address of the receiver.
      subject: The subject of the email message.
      message_text: The text of the email message.
    
    Returns:
      An object containing a base64url encoded email object.
    """
    message = MIMEText(message_text,'html')
    #message = MIMEText('text','html')
    
    message['to'] = to
    message['from'] = sender
    message['bcc'] = 'golgothen@gmail.com'
    message['subject'] = subject
    
    #return {'raw': encoders.encode_base64(message).decode()}
    #return {'raw': encoders.encode_base64(message.as_bytes()).decode()}
    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

def CreateMessageWithAttachment(sender, to, subject, message_text, file_dir, filename):
    """Create a message for an email.
    
    Args:
      sender: Email address of the sender.
      to: Email address of the receiver.
      subject: The subject of the email message.
      message_text: The text of the email message.
      file_dir: The directory containing the file to be attached.
      filename: The name of the file to be attached.
    
    Returns:
      An object containing a base64url encoded email object.
    """
    message = MIMEMultipart()
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    
    msg = MIMEText(message_text,'html')
    message.attach(msg)
    path = os.path.join(file_dir, filename)
    msg = MIMEBase('application','octet-stream')
    msg.set_payload(open(path, 'rb').read())
    msg.add_header('Content-Disposition', 'attachment', filename=filename)
    message.attach(msg)
    #return {'raw': message.as_string()}
    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
