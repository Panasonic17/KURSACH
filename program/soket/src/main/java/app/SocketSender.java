package app;

import com.satori.rtm.*;
import com.satori.rtm.model.AnyJson;
import com.satori.rtm.model.SubscriptionData;

import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;


public class SocketSender {
    public static void main(String[] args) throws IOException {
        ServerSocket echoServer = new ServerSocket(1500);
        Socket clientSocket = echoServer.accept();
        final PrintStream os = new PrintStream(clientSocket.getOutputStream());
        System.out.println("accept");

        String endpoint = "wss://open-data.api.satori.com";
        String appkey = "9fbd1c4BEa889C66cFf83B042B0fDCed";
        String channel = "air-traffic";

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
                    os.println(json);
                }
            }
        };
        client.createSubscription(channel, SubscriptionMode.SIMPLE, listener);

        client.start();
    }


}
