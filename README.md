# JedisBinaryPubSub example
## The connection information is set using the  jedisconnectionfactory.properties file

(so edit that file to match your environment)

This program demonstrates what happens when you utilize redis pubsub functionality using bytes for the payload. 
The default behavior (no args passed in) is for the program to 
1) start up 3 subscribers to a channel
2) wait a bit for them to register
3) publish 100 messages to that channel
4) when all subscribers have received all expected messages (100 by default) the program exits

#### To run this application in default mode you execute:
```
mvn compile exec:java
```

#### you may add args as follows:
```
mvn compile exec:java -Dexec.args="1000 10 true"
```
#### first arg is MAX_MESSAGES_FOR_TEST  (both publisher and listeners use this value)
#### second arg is HOW_MANY_LISTENERS (could even be 0 if your listeners are running elsewhere)
#### third is SHOULD_PUBLISH (maybe you only want to kick off listeners)

```
mvn compile exec:java -Dexec.args="10000 15 false"
```

