#!/bin/bash
echo "Starting Item-Kafka-Producer-POC !"
ipad=$( hostname -I | awk '{print $1}' )
localip=$( hostname -i )
echo "The ip is :  $ipad"

    echo "The local ip is : $localip"
    echo "Please specify your own config here."
    $JAVA_HOME_11/bin/java -Xms64m -Xmx128m -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=70 -jar -Dspring.config.location=classpath:/application.yml target/Item-Kafka-Producer-POC-0.0.1-SNAPSHOT.jar #> /dev/null 2>&1 &

#ps -ef | grep EF-Hello_World-Agent-POC
