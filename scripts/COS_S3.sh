
# Provide the following variables as script parameters

# S3_ACCESS_KEY=<AccessKey-changeme>
S3_ACCESS_KEY=$1

# S3_ENDPOINT=<EndPoint-changeme>
S3_ENDPOINT=$2

# S3_SECRET_KEY=<SecretKey-changeme>
S3_SECRET_KEY=$3

if [ "x$NODE_TYPE" == "xmaster-management" ]
then
    echo $AMBARI_USER:$AMBARI_PASSWORD:$AMBARI_HOST:$AMBARI_PORT:$CLUSTER_NAME

    echo "Node type is xmanagement hence updating ambari properties"
    /var/lib/ambari-server/resources/scripts/configs.sh -u $AMBARI_USER -p $AMBARI_PASSWORD -port $AMBARI_PORT -s set $AMBARI_HOST $CLUSTER_NAME  core-site "fs.s3a.access.key" $S3_ACCESS_KEY
    /var/lib/ambari-server/resources/scripts/configs.sh -u $AMBARI_USER -p $AMBARI_PASSWORD -port $AMBARI_PORT -s set $AMBARI_HOST $CLUSTER_NAME  core-site "fs.s3a.endpoint" $S3_ENDPOINT
    /var/lib/ambari-server/resources/scripts/configs.sh -u $AMBARI_USER -p $AMBARI_PASSWORD -port $AMBARI_PORT -s set $AMBARI_HOST $CLUSTER_NAME  core-site "fs.s3a.secret.key" $S3_SECRET_KEY

    echo "stop and Start Services"
    curl -k -v --user $AMBARI_USER:$AMBARI_PASSWORD -H "X-Requested-By: ambari" -i -X PUT -d '{"RequestInfo": {"context": "Stop All Services via REST"}, "ServiceInfo": {"state":"INSTALLED"}}' https://$AMBARI_HOST:$AMBARI_PORT/api/v1/clusters/$CLUSTER_NAME/services
    sleep 200

    curl -k -v --user $AMBARI_USER:$AMBARI_PASSWORD -H "X-Requested-By: ambari" -i -X PUT -d '{"RequestInfo": {"context": "Start All Services via REST"}, "ServiceInfo":{"state":"STARTED"}}' https://$AMBARI_HOST:$AMBARI_PORT/api/v1/clusters/$CLUSTER_NAME/services
    sleep 700
fi
