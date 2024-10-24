package fr.circleit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        MessagePublisher publisher = new MessagePublisher();

        Runnable runnable1 = () -> {
            log.debug("Publishing from thread 1");
            publisher.publishMessage("Message from thread 1",
                succeeded -> System.out.println("Thread 1 succeeded: " + succeeded),
                failed -> System.out.println("Thread 1 failed: " + failed));
        };

        Runnable runnable2 = () -> {
            log.debug("Publishing from thread 2");
            publisher.publishMessage("Message from thread 2",
                succeeded -> System.out.println("Thread 2 succeeded: " + succeeded),
                failed -> System.out.println("Thread 2 failed: " + failed));
        };

        new Thread(runnable1).start();
        new Thread(runnable2).start();
    }
}