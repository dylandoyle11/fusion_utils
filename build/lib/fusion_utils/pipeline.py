import errors
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import bigquery


class Pipeline:
    def __init__(self, name, QA_flag=True):
        self.name = name
        self.stages = []
        self.QA = QA_flag
        self.tasks = []
        self.client = bigquery.Client(project='aic-production-core')
        self.initialize_datasets()

    def set_table_map(self, dataset, table):
        table_map = f'`{dataset}.{table}`'
        self.table_map_df = self.client.query(f'select * from {table_map}').to_dataframe()
        
    
    def get_id_from_alias(self, alias):
        '''
        Given table alias, return QA/PROD dataset ID
        '''
        if not hasattr(self, 'table_map_df'):
            raise errors.TableMappingError
        
        row = self.table_map_df[self.table_map_df['alias'] == alias]

        if row.empty:
            raise ValueError(f"Alias '{alias}' not found in the datasets.")
        
        if self.QA:
            return row['qa_dataset'].values[0]
        else:
            return row['prod_dataset'].values[0]
        

    def initialize_datasets(self):
        for _, row in self.table_map_df.iterrows():
            alias = row['alias']
            dataset = row['qa_dataset'] if self.qa else row['prod_dataset']
            setattr(self, alias, dataset)


    def set_smtp_ip(self, dataset, table):
        smtp_map = f'`{dataset}.{table}`'
        try:
            self.smtp_ip = self.client.query(f'select ip from {smtp_map}')
        except Exception as e:
            raise e('Cannot retrieve smtp server IP.')


    def send_email(self, subject, body, sender, recipients):
        """
        Send an email using SMTP to multiple recipients.
        """
        if not hasattr(self, 'smtp_ip'):
            raise errors.SMTPConfigurationError

        message = MIMEMultipart()
        message['From'] = sender
        message['To'] = ", ".join(recipients)
        message['Subject'] = subject
        css = '<style>.pass {{ color: #008000; }} .fail {{ color: #FF0000; }}</style>'
        body_html = f"<html><head>{css}</head><body>{body}</body></html>"
        message.attach(MIMEText(body_html, 'html'))
        
        try:
            server = smtplib.SMTP(self.smtp_ip, 25)
            server.ehlo()  # Necessary for some SMTP servers
            server.sendmail(sender, recipients, message.as_string())
            server.quit()
            print("Email sent successfully.")
        except Exception as e:
            print("Failed to send email:", str(e))


 
 
pipeline = Pipeline('Fusion_test')