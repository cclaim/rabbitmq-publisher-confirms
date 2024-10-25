package fr.circleit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class MessagePublisher {
    private final ConnectionFactory factory;
    private Connection connection;
    private ThreadLocal<MessagePublisherChannel> publishChannel = new ThreadLocal<>();

    public MessagePublisher() {
        this.factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
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
            MessagePublisherChannel mpc = getPublishChannel(onSucceededPublish, onFailedPublish);
            mpc.trackMessage(message);
            Channel ch = mpc.getChannel();
            ch.basicPublish(
                "",
                routingKey,
                MessageProperties.PERSISTENT_BASIC,
                message.getBytes(StandardCharsets.UTF_8)
            );
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private MessagePublisherChannel getPublishChannel(
        Consumer<List<String>> onSucceededPublish, Consumer<List<String>> onFailedPublish
    ) throws IOException, TimeoutException {
        if (publishChannel.get() == null) {
            Channel channel = getConnection().createChannel();
            channel.confirmSelect(); // channel with ack
            MessagePublisherChannel mpc = new MessagePublisherChannel(channel);
            channel.addConfirmListener((sequenceNumber, multiple) -> {
                List<String> acked = mpc.cleanOutstandingConfirms(sequenceNumber, multiple);
                onSucceededPublish.accept(acked);
            }, (sequenceNumber, multiple) -> {
                List<String> nacked = mpc.cleanOutstandingConfirms(sequenceNumber, multiple);
                onFailedPublish.accept(nacked);
            });
            publishChannel.set(mpc);
        }
        return publishChannel.get();
    }
}
