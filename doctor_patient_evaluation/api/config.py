import os

CONFIG = {
    "prod":{
        "endpoint":"",
        "auth":os.environ.get('DS_PROD_AUTH', 'Basic ZHNfdXNlcjpkc0AxMjM=')
    },
    "stage":{
        "endpoint":"/ds/api/staging",
        "auth":os.environ.get('DS_STAGE_AUTH', 'Basic ZHNfdXNlcjpkc0AxMjM=')
    },
    "uat":{
        "endpoint":"/ds/api/uat",
        "auth":os.environ.get('DS_UAT_AUTH', 'Basic ZHNfdXNlcjpkc0AxMjM=')
    },
    "experiment":{
        "endpoint":"/ds/api/experiment",
        "auth":os.environ.get('DS_EXP_AUTH', 'Basic ZHNfdXNlcjpkc0AxMjM=')
    }
}