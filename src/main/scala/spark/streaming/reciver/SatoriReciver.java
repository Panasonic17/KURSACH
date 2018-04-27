package spark.streaming.reciver;

import com.satori.rtm.*;
import com.satori.rtm.model.AnyJson;
import com.satori.rtm.model.SubscriptionData;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class SatoriReciver extends Receiver<String> {

    String endpoint;
    String appkey;
    String channel;


    public SatoriReciver(String endpoint, String appkey, String channel) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.endpoint = endpoint;
        this.appkey = appkey;
        this.channel = channel;
    }

    @Override
    public void onStart() {
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {

    }

    private void receive() {
        final RtmClient client = new RtmClientBuilder(endpoint, appkey)
                .setListener(new RtmClientAdapter() {
                    @Override
                    public void onEnterConnected(RtmClient client) {
                        System.out.println("Connected to Satori RTM!");
                    }
                })
                .build();
        SubscriptionAdapter listener = new SubscriptionAdapter() {
            @Override
            public void onSubscriptionData(SubscriptionData data) {

                for (AnyJson json : data.getMessages()) {

                    store(json.toString());

                }
            }
        };
        client.createSubscription(channel, SubscriptionMode.SIMPLE, listener);
        client.start();
    }
}
