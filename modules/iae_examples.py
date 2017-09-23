import requests
import json

def set_notebook_full_width():
    from IPython.core.display import display, HTML
    display(HTML("<style>.container { width:100% !important; }</style>"))
    
    
def save_string_to_cos(string_data, bucket_name, filename, S3_ACCESS_KEY, S3_SECRET_KEY, S3_PUBLIC_ENDPOINT):

    import tempfile
    tmp_file, tmp_filename = tempfile.mkstemp()
    
    with open(tmp_filename,'wb') as output:
          output.write(string_data)
            
    import boto
    import boto.s3.connection

    conn = boto.connect_s3(
            aws_access_key_id = S3_ACCESS_KEY,
            aws_secret_access_key = S3_SECRET_KEY,
            host = S3_PUBLIC_ENDPOINT,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            )
    
    try:
        bucket = conn.get_bucket(bucket_name)
    except:
        bucket = conn.create_bucket(bucket_name)

    key = bucket.new_key(filename)
    key.set_contents_from_filename(tmp_filename)
    
    import os
    os.remove(tmp_filename)
    
def save_url_to_cos(url, bucket_name, filename, S3_ACCESS_KEY, S3_SECRET_KEY, S3_PUBLIC_ENDPOINT):
    
    import tempfile
    tmp_file, tmp_filename = tempfile.mkstemp()
    
    import urllib2
    fileobj = urllib2.urlopen(url)
    with open(tmp_filename,'wb') as output:
          output.write(fileobj.read())
            
    import boto
    import boto.s3.connection

    conn = boto.connect_s3(
            aws_access_key_id = S3_ACCESS_KEY,
            aws_secret_access_key = S3_SECRET_KEY,
            host = S3_PUBLIC_ENDPOINT,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            )
    
    try:
        bucket = conn.get_bucket(bucket_name)
    except:
        bucket = conn.create_bucket(bucket_name)

    key = bucket.new_key(filename)
    key.set_contents_from_filename(tmp_filename)
    
    import os
    os.remove(tmp_filename)
    
def strip_premable_from_service_key(service_key_file):
    
    # read the file
    with open(service_key_file) as data_file:
        # remove the first 4 lines
        data = ''.join(data_file.readlines()[4:])
    
    # save the file
    with open(service_key_file, 'w') as data_file:
        data_file.write(data)

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

    import json
    
    with open(cos_endpoint_filename) as data_file:    
        cos_s3_endpoint = json.load(data_file)

    S3_ACCESS_KEY       = cos_s3_endpoint['S3_ACCESS_KEY']
    S3_PRIVATE_ENDPOINT = cos_s3_endpoint['S3_PRIVATE_ENDPOINT']
    S3_PUBLIC_ENDPOINT  = cos_s3_endpoint['S3_PUBLIC_ENDPOINT']
    S3_SECRET_KEY       = cos_s3_endpoint['S3_SECRET_KEY']

    print('S3_PRIVATE_ENDPOINT: ' + S3_PRIVATE_ENDPOINT)
    print('S3_PUBLIC_ENDPOINT:  ' + S3_PUBLIC_ENDPOINT)
    
    return (S3_ACCESS_KEY, S3_PRIVATE_ENDPOINT, S3_PUBLIC_ENDPOINT, S3_SECRET_KEY)

def read_iae_service_keys(service_key_filename):

    import json

    with open(service_key_filename) as data_file:    
        iae_service_key = json.load(data_file)

    livy   = iae_service_key['cluster']['service_endpoints']['livy']
    ambari = iae_service_key['cluster']['service_endpoints']['ambari_console']
    user   = iae_service_key['cluster']['user']
    pswd   = iae_service_key['cluster']['password']

    #print('livy:   '   + livy)
    #print('ambari: '   + ambari)
    #print('user:   '   + user)
    
    return iae_service_key

def iae_service_user(service_key_filename):
    return read_iae_service_keys(service_key_filename)['cluster']['user']
    
def iae_service_password(service_key_filename):
    return read_iae_service_keys(service_key_filename)['cluster']['password']
    
def iae_service_endpoint_ambari(service_key_filename):
    return read_iae_service_keys(service_key_filename)['cluster']['service_endpoints']['ambari_console']
    
def iae_service_endpoint_livy(service_key_filename):
    return read_iae_service_keys(service_key_filename)['cluster']['service_endpoints']['livy']
    
def iae_service_endpoint_webhdfs(service_key_filename):
    return read_iae_service_keys(service_key_filename)['cluster']['service_endpoints']['webhdfs']
    