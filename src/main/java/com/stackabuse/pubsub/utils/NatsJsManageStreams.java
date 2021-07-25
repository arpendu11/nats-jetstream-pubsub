package com.stackabuse.pubsub.utils;

import java.time.Duration;
import java.util.List;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.PurgeResponse;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

public class NatsJsManageStreams {

    private static final String STREAM1 = "manage-stream1";
    private static final String STREAM2 = "manage-stream2";
    private static final String SUBJECT1 = "manage-subject1";
    private static final String SUBJECT2 = "manage-subject2";
    private static final String SUBJECT3 = "manage-subject3";
    private static final String SUBJECT4 = "manage-subject4";
    private static final String defaultServer = "nats://localhost:4222";

    public static void main(String[] args) {

        try (Connection nc = Nats.connect(defaultServer)) {

            // Create a JetStreamManagement context.
            JetStreamManagement jsm = nc.jetStreamManagement();

            // 1. Create (add) a stream with a subject
            // -  Full configuration schema:
            //    https://github.com/nats-io/jsm.go/blob/main/schemas/jetstream/api/v1/stream_configuration.json
            System.out.println("\n----------\n1. Configure And Add Stream 1");
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(STREAM1)
                    .subjects(SUBJECT1)
                    // .retentionPolicy(...)
                    // .maxConsumers(...)
                    // .maxBytes(...)
                    // .maxAge(...)
                    // .maxMsgSize(...)
                    .storageType(StorageType.Memory)
                    // .replicas(...)
                    // .noAck(...)
                    // .template(...)
                    // .discardPolicy(...)
                    .build();
            StreamInfo streamInfo = jsm.addStream(streamConfig);
            NatsJsUtils.printStreamInfo(streamInfo);

            // 2. Update stream, in this case add a subject
            // -  StreamConfiguration is immutable once created
            // -  but the builder can help with that.
            System.out.println("----------\n2. Update Stream 1");
            streamConfig = StreamConfiguration.builder(streamInfo.getConfiguration())
                    .addSubjects(SUBJECT2).build();
            streamInfo = jsm.updateStream(streamConfig);
            NatsJsUtils.printStreamInfo(streamInfo);

            // 3. Create (add) another stream with 2 subjects
            System.out.println("----------\n3. Configure And Add Stream 2");
            streamConfig = StreamConfiguration.builder()
                    .name(STREAM2)
                    .storageType(StorageType.Memory)
                    .subjects(SUBJECT3, SUBJECT4)
                    .build();
            streamInfo = jsm.addStream(streamConfig);
            NatsJsUtils.printStreamInfo(streamInfo);

            // 4. Get information on streams
            // 4.0 publish some message for more interesting stream state information
            // -   SUBJECT1 is associated with STREAM1
            // 4.1 getStreamInfo on a specific stream
            // 4.2 get a list of all streams
            // 4.3 get a list of StreamInfo's for all streams
            System.out.println("----------\n4.1 getStreamInfo");
            NatsJsUtils.publish(nc, SUBJECT1, 5);
            streamInfo = jsm.getStreamInfo(STREAM1);
            NatsJsUtils.printStreamInfo(streamInfo);

            System.out.println("----------\n4.2 getStreamNames");
            List<String> streamNames = jsm.getStreamNames();
            NatsJsUtils.printObject(streamNames);

            System.out.println("----------\n4.2 getStreamNames");
            List<StreamInfo> streamInfos = jsm.getStreams();
            NatsJsUtils.printStreamInfoList(streamInfos);

            // 5. Purge a stream of it's messages
            System.out.println("----------\n5. Purge stream");
            PurgeResponse purgeResponse = jsm.purgeStream(STREAM1);
            NatsJsUtils.printObject(purgeResponse);

            // 6. Delete a stream
            // Subsequent calls to getStreamInfo, deleteStream or purgeStream
            // will throw a JetStreamApiException "stream not found (404)"
            System.out.println("----------\n6. Delete stream");
            jsm.deleteStream(STREAM2);

            System.out.println("----------\n");
            
            // Make sure the message goes through before we close
            nc.flush(Duration.ZERO);
            nc.close();
        }
        catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}
