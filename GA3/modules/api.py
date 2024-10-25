from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

class GA3Service:
    def __init__(self,scopes,key_file_location,view_id) -> None:
        self.scopes=scopes
        self.key_file_location=key_file_location
        self.view_id=view_id

    def getBody(self,startDate:str,endDate:str,dimensions:list,metrics:list):
        body = {
                'reportRequests': [
                {
                'viewId': self.view_id,
                'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
                'metrics': [],
                'dimensions': []
                }]
            }
        # {'name': 'ga:country'}
        # {'expression': 'ga:sessions'}
        dimensionkey:list = body['reportRequests'][0]['dimensions']
        metricskey:list = body['reportRequests'][0]['metrics']
        for dimension in dimensions:
           dimensionkey.append({'name':dimension})
        for metric in metrics:
            metricskey.append({'expression':metric})

        return body
        

    def initialize_analyticsreporting(self):
        """Initializes an Analytics Reporting API V4 service object.
        Returns:
        An authorized Analytics Reporting API V4 service object."""
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.key_file_location,self.scopes)

        # Build the service object.
        analytics = build('analyticsreporting', 'v4', credentials=credentials)

        return analytics
    
    @staticmethod
    def get_report(analytics,body):
        """Queries the Analytics Reporting API V4.

        Args:
            analytics: An authorized Analytics Reporting API V4 service object.
        Returns:
            The Analytics Reporting API V4 response.
        """
        response = analytics.reports().batchGet(body=body).execute()
        return response
