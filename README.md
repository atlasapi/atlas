Build and Run Atlas
===================

The Atlas source-code is hosted at [github.com/atlasapi](http://github.com/atlasapi) and is split across 5 projects:

* [github.com/atlasapi/atlas](http://github.com/atlasapi/atlas)
* [github.com/atlasapi/atlas-persistence](http://github.com/atlasapi/atlas-persistence)
* [github.com/atlasapi/atlas-model](http://github.com/atlasapi/atlas-model)
* [github.com/atlasapi/atlas-feeds](http://github.com/atlasapi/atlas-feeds)

Additionally, [github.com/atlasapi/atlas-client](http://github.com/atlasapi/atlas-client) hosts the Java client library. [github.com/atlasapi/atlas](http://github.com/atlasapi/atlas) is the main entry point and you don't need the others unless you're interested in updating them too.

## MongoDB

Atlas, and specifically atlas-persistence, uses [MongoDB](http://www.mongodb.org/) to store its indexed content. Atlas doesn't come packaged with MongoDB so you'll need to make sure it's installed - [Download MongoDB](http://www.mongodb.org/downloads).

During test runs, Atlas will run integration tests against a mongo running on port 8585. If one isn't running then it'll try and start one, assuming that mongod is available on its path, so either keep one running or make sure you've added mongo/bin to your path.

The running Atlas instance requires that MongoDB be running on its standard port:27107 and ZooKeeper/Kafka: running locally.  Please make sure you've kicked them off.

## Maven

Atlas uses [Maven](http://maven.apache.org/) for all it's dependency and build management, so you'd better have mvn available on your path! We've included the MetaBroadcast public repo which houses all the dependencies that haven't been mavenised, and all our successful builds deploy to it so it has the latest atlas SNAPSHOTs available. This means you don't have to build the other atlas projects, if you don't want to.

It's worth noting that we don't current have a formal release process and everything's currently a SNAPSHOT release. We're sorry if this is a pain and we have every intention of creating some proper releases soon, when life has calmed down a bit.

## Building and Running

So, to get everything built and ready:

    mkdir /data                                      # Required for feed processing
    git clone http://github.com/atlasapi/atlas.git
    cd atlas
    mvn clean install
    
This will download all the dependencies, compile the code and run the tests (make sure mongo's setup). To actually run the project locally:

    mvn jetty:run -Dprocessing.config=true -Dupdaters.bbc.enabled=true -Djetty.port=8282 # Atlas processing
    mvn jetty:run -Dprocessing.config=false                                              # Atlas front-end
    
This will startup Atlas locally using the lovely [Jetty](http://jetty.codehaus.org/jetty/).

Go to

    http://localhost:8282/system/scheduler

and run "BBC Ion schedule update (today only)" to ingest the BBC schedule for today. When that completes, run "BBC Mongo Schedule repopulator" to generate the schedule. 

Then go to

    http://localhost:8080/3.0/schedule.json?from=now.minus.3h&to=now.plus.10h&channel=bbcone&publisher=bbc.co.uk 

for some of today's BBC One schedule. You may need to modify the 'from' and 'to' parameters, depending on the time of day.

Enjoy!

## Contributions

We welcome contributions to Atlas! If you'd like to get your hands dirty, please [fork the repositories](https://help.github.com/articles/fork-a-repo) and submit [pull requests](https://help.github.com/articles/using-pull-requests) with your changes. 
