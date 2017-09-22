import requests
import json

def set_notebook_full_width():
    from IPython.core.display import display, HTML
    display(HTML("<style>.container { width:100% !important; }</style>"))

def get_cluster_name(ambari_url, user, password):
    
    url = ambari_url + '/api/v1/clusters'
    auth = (user, password)
    headers = { "X-Requested-By": "ambari" }

    response = requests.get(url, auth=auth, headers=headers)

    cluster_name = response.json()['items'][0]['Clusters']['cluster_name']
    return cluster_name


def is_s3_access_key_set(ambari_url, user, password, s3_access_key_set):

    cluster_name = get_cluster_name(ambari_url, user, password)
    url = ambari_url + '/api/v1/clusters/{0}/configurations/service_config_versions'.format(cluster_name)
    auth = (user, password)
    headers = { "X-Requested-By": "ambari" }

    response = requests.get(url, auth=auth, headers=headers)
    j = response.json()

    from objectpath import Tree
    tree=Tree(j)

    items = tree.execute("$..configurations.*[@.type is 'core-site']")

    for i in items:
        if 'fs.s3a.access.key' not in i['properties']:
            return False
        
        if s3_access_key_set != i['properties']['fs.s3a.access.key']:
            return False
            
    return True

    # fs.s3a.access.key
    # fs.s3a.endpoint
    # fs.s3a.secret.key
    

def read_cf_target_endpoint_details(target_endpoint_filename):

    with open(target_endpoint_filename) as data_file:    
        target_endpoint = json.load(data_file)

    api   = target_endpoint['api']
    org   = target_endpoint['org']
    space = target_endpoint['space']

    print('CF_API:   ' + api)
    print('CF_ORG:   ' + org)
    print('CF_SPACE: ' + space)
    
    return (api, org, space)

def read_cos_endpoint_details(cos_endpoint_filename):

    with open(cos_endpoint_filename) as data_file:    
        cos_s3_endpoint = json.load(data_file)

    S3_ACCESS_KEY       = cos_s3_endpoint['S3_ACCESS_KEY']
    S3_PRIVATE_ENDPOINT = cos_s3_endpoint['S3_PRIVATE_ENDPOINT']
    S3_PUBLIC_ENDPOINT  = cos_s3_endpoint['S3_PUBLIC_ENDPOINT']
    S3_SECRET_KEY       = cos_s3_endpoint['S3_SECRET_KEY']

    print('S3_PRIVATE_ENDPOINT: ' + S3_PRIVATE_ENDPOINT)
    print('S3_PUBLIC_ENDPOINT:  ' + S3_PUBLIC_ENDPOINT)
    
    return (S3_ACCESS_KEY, S3_PRIVATE_ENDPOINT, S3_PUBLIC_ENDPOINT, S3_SECRET_KEY)