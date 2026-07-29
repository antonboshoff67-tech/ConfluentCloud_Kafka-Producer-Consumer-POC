#!/bin/bash
#pid=$( ps -ef|grep java|grep Leads-Service|awk {'print $2'} );
#if [ "$pid" != "" ];
#    then kill $pid;
#fi;
#!/bin/bash

kill -9 `ps -ef | grep java | grep EF-Hello_World-Agent-POC | awk {'print $2'}`
