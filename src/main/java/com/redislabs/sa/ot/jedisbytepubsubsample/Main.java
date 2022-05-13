package com.redislabs.sa.ot.jedisbytepubsubsample;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import org.apache.commons.lang3.SerializationUtils;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;

//start this program by executing:  mvn compile exec:java
//
// arguments to this program can alter values for:
// MAX_MESSAGES_FOR_TEST, HOW_MANY_LISTENERS, and SHOULD_PUBLISH

public class Main {

    public static final String CHANNEL_NAME = "someclazz:updates";
    public static final JedisPool jedisPool = JedisConnectionFactory.getInstance().getJedisPool();
    public static int MAX_MESSAGES_FOR_TEST=100; // not final - may use args in future so we can adjust to suit tester preferences
    public static int HOW_MANY_LISTENERS = 3; // not final - may use args in future so we can adjust to suit tester preferences
    public static boolean SHOULD_PUBLISH = true; // not final - may use args in future so we can adjust to suit tester preferences

    public static void main(String [] args){
        if(args.length>0){
            MAX_MESSAGES_FOR_TEST = Integer.parseInt(args[0]);
        }
        if(args.length>1){
            HOW_MANY_LISTENERS = Integer.parseInt(args[1]);
        }
        if(args.length>2){
            SHOULD_PUBLISH = Boolean.parseBoolean(args[2]);
        }
        Main main = new Main();
        main.startListeners(HOW_MANY_LISTENERS);
        try {
            Thread.sleep((HOW_MANY_LISTENERS*300)+7000); // give listenere time to subscribe
        }catch(InterruptedException ie){}//we don't expect this and don't care for this simple test
        if(SHOULD_PUBLISH) {
            for (int x = 1; x <= MAX_MESSAGES_FOR_TEST; x++) {
                main.publishMessage(x);
            }
        }
    }

    void startListeners(int howManyListeners){
        //Create connection to Redis Server:
        for(int x = 0;x<howManyListeners;x++){
            PubSubListener listener = new PubSubListener("Listener_"+x);
            Thread t = new Thread(listener);
            t.start();
        }
    }

    void publishMessage(int messageNumber) {
        try {
            byte[] channelName = SerializationUtils.serialize(Main.CHANNEL_NAME);
            SomeClazz sc = new SomeClazz();
            System.out.println("Created a new instance of SomeClazz... toString() results in:\n"+sc);
            //create a ByteArray (Serialized) version of our object suitable for passing around:
            byte[] message = SerializationUtils.serialize(sc);
            System.out.println("After local Serialization/de-Serialization... toString results in:\n"+SerializationUtils.deserialize(message));
            Long recipientCount;
            try (Jedis jedis = jedisPool.getResource()) {
                recipientCount = jedis.publish(channelName, message);
            }
            System.out.println("\t[[PUBLISHING]] # "+messageNumber+" --> Channel: " + SerializationUtils.deserialize(channelName) + " Message: " + SerializationUtils.deserialize(message) + " sent to " + recipientCount + " subscribers.");
            Thread.sleep(10);
        } catch (InterruptedException e) {
            System.err.println("Publisher interrupted during sleep.");
        }
    }
}


class SomeClazz implements Serializable {
    private static final long serialVersionUID=1L;
    private int someValue = (int) (System.currentTimeMillis()%500);
    private String stringValue = ((System.currentTimeMillis()%2)==0) ? "Happy Days Are Here Again" : "Cloudy & Rainy Sadness Pervades The Globe";

    public void setSomeValue(int sv){
        this.someValue = sv;
    }
    public int getSomeValue(){
        return this.someValue;
    }
    public void setStringValue(String sv){
        this.stringValue = sv;
    }
    public String getStringValue(){
        return this.stringValue;
    }

    public String toString() {
        return "My values are as follows... someValue = " + this.getSomeValue() + "  stringValue = " + this.getStringValue();
    }
}

class PubSubListener implements Runnable{

    byte[] channelName = SerializationUtils.serialize(Main.CHANNEL_NAME);
    BinaryJedisPubSub pubsub = null; // will be set during listenForMessages method
    int messageCounter = 0;
    String instanceID = "me";
    boolean shouldStop = false;

    public PubSubListener(String id){
        this.instanceID = id;
    }

    void updateMessageCount(){
        messageCounter++; // first message receieved will be seen as #1
        if(messageCounter >= Main.MAX_MESSAGES_FOR_TEST){ // DO this when all messages are received
            System.out.println("\n\t"+messageCounter + " Messages received by "+instanceID+"__"+System.identityHashCode(this));
            //reset messageCounter as the expected test load is complete:
            messageCounter = 0;
            shouldStop = true;
            pubsub.unsubscribe();
        }
    }

    @Override
    public void run() {
        listenForMessages(this);
        while(!shouldStop){
            //empty loop
            try {
                Thread.sleep(10);
            }catch(Throwable t){} // don't care
        }
    }

    void listenForMessages(PubSubListener parent){
        BinaryJedisPubSub pubSub = new BinaryJedisPubSub() {
            @Override
            public void onMessage(byte[] channel, byte[] message) {
                parent.updateMessageCount();
                System.out.println(parent.instanceID+" Received message # "+parent.messageCounter+" from Channel: " + SerializationUtils.deserialize(channel) + " Message: " + SerializationUtils.deserialize(message));
            }

            @Override
            public void onSubscribe(byte[] channel, int subscribedChannels) {
                System.out.println(parent.instanceID+" Subscribed to: " + SerializationUtils.deserialize(channel));
            }

            @Override
            public void onPMessage(byte[] pattern, byte[] channel, byte[] message) {
                System.out.println(parent.instanceID+" Pattern: " + SerializationUtils.deserialize(pattern) + " Channel: " + SerializationUtils.deserialize(channel) + " Message: " + SerializationUtils.deserialize(message));
            }

            @Override
            public void onPSubscribe(byte[] pattern, int subscribedChannels) {
                System.out.println(parent.instanceID+" Subscribed to: " + SerializationUtils.deserialize(pattern));
            }
        };
        parent.pubsub = pubSub;
        try (Jedis jedis = Main.jedisPool.getResource()) {
            jedis.subscribe(pubSub, channelName);
        }
    }
}
