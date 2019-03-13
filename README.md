# Dump1090Listener

Super simple Java based client for [dump1090](https://github.com/MalcolmRobb/dump1090) SBS1 (BaseStation) service. Collected messages are stored in [MongoDB](https://www.mongodb.com/).

## Prerequisites

You will need a running instance of Dump1090 as well as a MongoDB database. You will also need a recent [JVM](http://www.oracle.com/technetwork/java/javase/downloads/index.html) and [Maven](https://maven.apache.org/) to install and run this program. If you don't want to use Maven you can still manually compile the .java files and make sure to include the [MongoDB Java Driver](http://mongodb.github.io/mongo-java-driver/) in the classpath.

## Install and run

    git clone https://github.com/koen-aerts/Dump1090Listener.git
    cd Dump1090Listener
    mvn compile
    ./run.sh --help

## MongoDB collection

You can store the collected messages in any collection you wish. You may want to consider creating an index on the "position" field. For instance if your collection is called "messages" you would create the index as follows:

    db.messages.createIndex( { position : "2dsphere" } )

## More Info

[Mobia Technology Innovations](http://mobia.io/)
