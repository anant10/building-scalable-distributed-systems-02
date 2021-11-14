package mqconsumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Processor implements Runnable {

  private final static String QUEUE_NAME = "threadExQ";
  private Connection connection;
  private Map<String, List<String>> mapOfLiftRides;

  public Processor(Connection conn, Map<String, List<String>> mapOfLiftRides) {

    this.connection = conn;
    this.mapOfLiftRides = mapOfLiftRides;
  }

  public void run() {
    try {
      final Channel channel = connection.createChannel();
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);
      // max one message per receiver
      channel.basicQos(1);
      System.out.println(" [*] Thread waiting for messages. To exit press CTRL+C");

      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        String[] split = message.split(",");
        String[] time = split[0].split(":");
        String[] liftId = split[1].split(":");
        mapOfLiftRides.putIfAbsent(liftId[1], new ArrayList<String>());
        mapOfLiftRides.get(liftId[1]).add(time[1]);
        System.out.println( "Callback thread ID = " + Thread.currentThread().getId() + " Received '" + message + "'");
      };
      // process messages
      channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
    } catch (IOException ex) {
      Logger.getLogger(Processor.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}