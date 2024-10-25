package fr.circleit;

import com.rabbitmq.client.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MessagePublisherChannel {
    private final Channel channel;
    private final ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

    public MessagePublisherChannel(Channel channel) {
        this.channel = channel;
    }

    public Channel getChannel() {
        return channel;
    }

    public void trackMessage(String message) {
        long sequenceNumber = channel.getNextPublishSeqNo();
        outstandingConfirms.put(sequenceNumber, message);
    }

    public List<String> cleanOutstandingConfirms(long sequenceNumber, boolean multiple) {
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
