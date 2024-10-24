package fr.circleit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class MessagePublisher {
    private final ConnectionFactory factory;
    private Connection connection;
    private ThreadLocal<Channel> publishChannel = new ThreadLocal<>();
    private final ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

    public MessagePublisher() {
        this.factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("admin");
        factory.setPassword("password");
        factory.setPort(5672);
    }

    private Connection getConnection() throws IOException, TimeoutException {
        if (connection == null) {
            connection = factory.newConnection();
        }
        return connection;
    }

    public void publishMessage(
        String message, Consumer<List<String>> onSucceededPublish,
        Consumer<List<String>> onFailedPublish
    ) {
        String routingKey = "sample-route";
        try {
            Channel channel = getPublishChannel(onSucceededPublish, onFailedPublish);
            long sequenceNumber = channel.getNextPublishSeqNo();
            outstandingConfirms.put(sequenceNumber, message);
            channel.basicPublish(
                "",
                routingKey,
                MessageProperties.PERSISTENT_BASIC,
                message.getBytes(StandardCharsets.UTF_8)
            );
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private Channel getPublishChannel(
        Consumer<List<String>> onSucceededPublish, Consumer<List<String>> onFailedPublish
    ) throws IOException, TimeoutException {
        if (publishChannel.get() == null) {
            Channel channel = getConnection().createChannel();
            channel.confirmSelect(); // channel with ack
            channel.addConfirmListener((sequenceNumber, multiple) -> {
                List<String> acked = cleanOutstandingConfirms(sequenceNumber, multiple);
                onSucceededPublish.accept(acked);
            }, (sequenceNumber, multiple) -> {
                List<String> nacked = cleanOutstandingConfirms(sequenceNumber, multiple);
                onFailedPublish.accept(nacked);
            });
            publishChannel.set(channel);
        }
        return publishChannel.get();
    }

    private List<String> cleanOutstandingConfirms(long sequenceNumber, boolean multiple) {
        List<String> notifsConfirmed;
        if (multiple) {
            ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(sequenceNumber, true);
            notifsConfirmed = new ArrayList<>(confirmed.values());
            confirmed.clear();
        } else {
            String confirmed = outstandingConfirms.remove(sequenceNumber);
            notifsConfirmed = List.of(confirmed);
        }
        return notifsConfirmed;
    }


}
