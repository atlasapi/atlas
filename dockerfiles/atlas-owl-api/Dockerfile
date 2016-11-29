FROM 448613307115.dkr.ecr.eu-west-1.amazonaws.com/jvm-oracle:latest

ENV SERVER_PORT="8080" \
    LOG4J_CONFIGURATION="file:////usr/local/jetty/log4j.properties" \
    NIMROD_LOG_LEVEL="INFO" \
    ROOT_LOG_LEVEL="INFO" \
    JVM_MEMORY="5500m" \
    JVM_OPTS="-XX:+UseG1GC \
      -XX:MaxGCPauseMillis=150 \
      -XX:CMSInitiatingOccupancyFraction=60 \
      -XX:+ExitOnOutOfMemoryError"

RUN mkdir -p /mnt/equiv-results

COPY target/atlas.war /usr/local/jetty/atlas.war
COPY log4j.properties /usr/local/jetty/log4j.properties

WORKDIR /usr/local/jetty

CMD java \
    -Djetty.home="$JETTY_HOME" \
    -Dsun.net.inetaddr.ttl="$SUN_NET_INETADDR_TTL" \
    -DMBST_PLATFORM="$MBST_PLATFORM" \
    -DMONGO_POOLSIZE="$MONGO_POOLSIZE" \
    -Datlas.search.host="$ATLAS_SEARCH_HOST" \
    -Dcassandra.connectionTimeout="$CASSANDRA_CONNECTIONTIMEOUT" \
    -Dcassandra.port="$CASSANDRA_PORT" \
    -Dcassandra.requestTimeout="$CASSANDRA_REQUESTTIMEOUT" \
    -Dcassandra.seeds="$CASSANDRA_SEEDS" \
    -Dequiv.results.directory="$EQUIV_RESULTS_DIRECTORY" \
    -Dids.expose="$IDS_EXPOSE" \
    -Dids.generate="$IDS_GENERATE" \
    -Dlocal.host.name="$LOCAL_HOST_NAME" \
    -Dmessaging.broker.url="$MESSAGING_BROKER_URL" \
    -Dmessaging.destination.changes="$MESSAGING_DESTINATION_CHANGES" \
    -Dmessaging.destination.indexer="$MESSAGING_DESTINATION_INDEXER" \
    -Dmessaging.destination.logger="$MESSAGING_DESTINATION_LOGGER" \
    -Dmessaging.destination.replicator="$MESSAGING_DESTINATION_REPLICATOR" \
    -Dmessaging.enabled="$MESSAGING_ENABLED" \
    -Dmessaging.zookeeper="$MESSAGING_ZOOKEEPER" \
    -Dmongo.db.tag="$MONGO_DB_TAG" \
    -Dmongo.db.tag.fallback="$MONGO_DB_TAG_FALLBACK" \
    -Dmongo.dbName="$MONGO_DBNAME" \
    -Dmongo.host="$MONGO_HOST" \
    -Dprocessing.config="$PROCESSING_CONFIG" \
    -Dreporting.columbus-telescope.environment="$REPORTING_COLUMBUS-TELESCOPE_ENVIRONMENT" \
    -Dreporting.columbus-telescope.host="$REPORTING_COLUMBUS-TELESCOPE_HOST" \
    -Drequest.threads="$REQUEST_THREADS" \
    -Drp.ftp.enabled="$RP_FTP_ENABLED" \
    -Drp.ftp.manualUpload.enabled="$RP_FTP_MANUALUPLOAD_ENABLED" \
    -Drp.ftp.services="$RP_FTP_SERVICES" \
    -Drp.health.password="$RP_HEALTH_PASSWORD" \
    -Drp.https.baseUrl="$RP_HTTPS_BASEURL" \
    -Drp.https.manualUpload.enabled="$RP_HTTPS_MANUALUPLOAD_ENABLED" \
    -Drp.https.password="$RP_HTTPS_PASSWORD" \
    -Drp.https.services="$RP_HTTPS_SERVICES" \
    -Drp.https.username="$RP_HTTPS_USERNAME" \
    -Drp.upload.unique="$RP_UPLOAD_UNIQUE" \
    -Dserver.port="$SERVER_PORT" \
    -Dservice.web.id="$SERVICE_WEB_ID" \
    -Dsitemaps.c4.brightcove.playerId="$SITEMAPS_C4_BRIGHTCOVE_PLAYERID" \
    -Dsitemaps.c4.brightcove.publisherId="$SITEMAPS_C4_BRIGHTCOVE_PUBLISHERID" \
    -Dsitemaps.c4.flashplayerversion="$SITEMAPS_C4_FLASHPLAYERVERSION" \
    -Dsitemaps.c4.flashplayerversion.uri="$SITEMAPS_C4_FLASHPLAYERVERSION_URI" \
    -Dtwitter.auth.consumerKey="$TWITTER_AUTH_CONSUMERKEY" \
    -Dtwitter.auth.consumerSecret="$TWITTER_AUTH_CONSUMERSECRET" \
    -Dwatermark.channelUris="$WATERMARK_CHANNELURIS" \
    -Dyouview.upload.nitro.upload.enabled="$YOUVIEW_UPLOAD_NITRO_UPLOAD_ENABLED" \
    -Djsse.enableSNIExtension="$JSSE_ENABLESNIEXTENSION" \
    -Dhttp.nonProxyHosts="$HTTP_NONPROXYHOSTS" \
    -Dhttp.proxyHost="$HTTP_PROXYHOST" \
    -Dhttp.proxyPort="$HTTP_PROXYPORT" \
    -Dhttps.proxyHost="$HTTPS_PROXYHOST" \
    -Dhttps.proxyPort="$HTTPS_PROXYPORT" \
    -Dlog4j.configuration="$LOG4J_CONFIGURATION" \
    -Dnimrod.log.level="$NIMROD_LOG_LEVEL" \
    -Droot.log.level="$ROOT_LOG_LEVEL" \
    -Xmx$JVM_MEMORY \
    -Xms$JVM_MEMORY \
    $JVM_OPTS \
    -jar atlas.war
