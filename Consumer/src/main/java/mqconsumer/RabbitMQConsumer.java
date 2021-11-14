package mqconsumer;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class RabbitMQConsumer {
  // use concurrent hashmap and log the details in the map
  private final static String QUEUE_NAME = "threadExQ";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setVirtualHost("/");
    factory.setHost("34.224.25.232");
    factory.setPort(5672);
    final Connection connection = factory.newConnection();
    Map<String, List<String>> mapOfLiftRides = new ConcurrentHashMap<String, List<String>>();

    ExecutorService executor = Executors.newFixedThreadPool(300);//2 Threads
    for (int i = 0; i < 100; i++) { // call the (Processor(i).run) 2 times with 2 threads
      executor.submit(new Processor(connection, mapOfLiftRides));
    }
  }
}
