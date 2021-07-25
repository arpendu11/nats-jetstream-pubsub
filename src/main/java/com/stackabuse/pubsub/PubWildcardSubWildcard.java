package com.stackabuse.pubsub;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.JsonUtils;

public class PubWildcardSubWildcard {

	private static final String defaultStream = "pubsubwildcardasync-stream";
	private static final String defaultSubjectWildcard = "audit.us.*";
	private static final String defaultSubjectSpecific = "audit.us.east";
	private static final String defaultMessage = "Audit User";
	private static final int defaultMessageCount = 2;
	private static final String defaultServer = "nats://localhost:4222";
	
	public static void main( String[] args ) {
		System.out.printf("\nPublishing to %s. Server is %s\n\n", defaultSubjectWildcard, defaultServer);
		
		try (Connection nc = Nats.connect(defaultServer)) {
            
            JetStreamManagement jsm = nc.jetStreamManagement();
            
            // Create a stream, here will use an in-memory storage type, and one subject
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(defaultStream)
                    .storageType(StorageType.Memory)
                    .subjects(defaultSubjectWildcard)
                    .build();
            
            // Add a stream.
            StreamInfo streamInfo = jsm.addStream(sc);
            JsonUtils.printFormatted(streamInfo);

            // Create a JetStream context.  This hangs off the original connection
            // allowing us to produce data to streams and consume data from
            // JetStream consumers.
            JetStream js = nc.jetStream();            

            // Create a future for asynchronous message processing
            List<CompletableFuture<PublishAck>> futures = new ArrayList<>();
            int stop = defaultMessageCount + 1;
            for (int x = 1; x < stop; x++) {
                String data = defaultMessage + "-" + x;

                // create a typical NATS message
                Message msg = NatsMessage.builder()
                        .subject(defaultSubjectSpecific)
                        .data(data, StandardCharsets.UTF_8)
                        .build();
                System.out.printf("Publishing message %s on subject %s.\n", data, defaultSubjectSpecific);

                // Publish a message
                futures.add(js.publishAsync(msg));
            }

            // Get Acknowledgement for the messages
            while (futures.size() > 0) {
                CompletableFuture<PublishAck> f = futures.remove(0);
                if (f.isDone()) {
                    try {
                        PublishAck pa = f.get();
                        System.out.printf("Publish Succeeded on subject %s, stream %s, seqno %d.\n",
                        		defaultSubjectSpecific, pa.getStream(), pa.getSeqno());
                    }
                    catch (ExecutionException ee) {
                        System.out.println("Publish Failed " + ee);
                    }
                }
                else {
                    // re queue it and try again
                    futures.add(f);
                }
            }
            
            // Subscribe messages that have been published to the subject
            JetStreamSubscription sub = js.subscribe(defaultSubjectWildcard);
            List<Message> messages = new ArrayList<>();
            Message msg = sub.nextMessage(Duration.ofSeconds(1));
            boolean first = true;
            while (msg != null) {
                if (first) {
                    first = false;
                    System.out.print("Read/Ack ->");
                }
                messages.add(msg);
                if (msg.isJetStream()) {
                    msg.ack();
                    System.out.print(" " + new String(msg.getData()) + "\n");
                    
                }
                else if (msg.isStatusMessage()) {
                        System.out.print(" !" + msg.getStatus().getCode() + "!");
                }
                JsonUtils.printFormatted(msg.metaData());
                msg = sub.nextMessage(Duration.ofSeconds(1));
            }

        	// Make sure the message goes through before we close
            nc.flush(Duration.ZERO);
            nc.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
	}
}
