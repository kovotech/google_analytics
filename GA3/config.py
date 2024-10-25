# Project Configurations
viewid_1 = ''
viewid_2 = ''
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
VIEW_ID = viewid_2
KEY_FILE_LOCATION = f"path\\to\\ga3_credentials.json"

# Table Configurations 
All_Session_by_Channels = {
    "dimensions":["ga:date","ga:channelGrouping"],
    "metrics" : ["ga:sessions","ga:users"]
}

Google_Ads1 = {
    "dimensions":["ga:date"],
    "metrics" : ["ga:adClicks","ga:adCost","ga:CPC","ga:CPM","ga:impressions","ga:sessions","ga:users"]
}

Session_Social = {
    "dimensions":["ga:channelGrouping","ga:date","ga:sourceMedium"],
    "metrics" : ["ga:sessions"]
}

sql_credentials = {
    "host":"",
    "db":"",
    "user":"",
    "pass":""
}