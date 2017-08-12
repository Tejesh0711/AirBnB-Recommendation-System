package actors;

import akka.actor.ActorRef;
import kafkaClients.KafkaRecommendationResponseConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Created by akashnagesh on 4/19/17.
 */
public class ConsumerThread implements Runnable {

    private final ActorRef actorRef;
    private final KafkaRecommendationResponseConsumer consumerClient;

    public ConsumerThread(ActorRef actorRef, KafkaRecommendationResponseConsumer consumerClient) {
        this.actorRef = actorRef;
        this.consumerClient = consumerClient;
    }

    @Override
    public void run() {

        while (true) {
            ConsumerRecords<String, String> records = consumerClient.consumeMessage();
            actorRef.tell(new ConsumerActor.ConsumedRecords(records), null);
        }
    }
}
