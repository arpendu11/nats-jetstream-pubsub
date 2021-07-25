package com.stackabuse.pubsub.utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;

class SSLUtils {
    public static String KEYSTORE_PATH = "keystore.jks";
    public static String TRUSTSTORE_PATH = "truststore.jks";
    public static String STORE_PASSWORD = "password";
    public static String KEY_PASSWORD = "password";
    public static String ALGORITHM = "SunX509";

    public static KeyStore loadKeystore(String path) throws Exception {
        KeyStore store = KeyStore.getInstance("JKS");
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(path));

        try {
            store.load(in, STORE_PASSWORD.toCharArray());
        } finally {
            if (in != null) {
                in.close();
            }
        }

        return store;
    }

    public static KeyManager[] createTestKeyManagers() throws Exception {
        KeyStore store = loadKeystore(KEYSTORE_PATH);
        KeyManagerFactory factory = KeyManagerFactory.getInstance(ALGORITHM);
        factory.init(store, KEY_PASSWORD.toCharArray());
        return factory.getKeyManagers();
    }

    public static TrustManager[] createTestTrustManagers() throws Exception {
        KeyStore store = loadKeystore(TRUSTSTORE_PATH);
        TrustManagerFactory factory = TrustManagerFactory.getInstance(ALGORITHM);
        factory.init(store);
        return factory.getTrustManagers();
    }

    public static SSLContext createSSLContext() throws Exception {
        SSLContext ctx = SSLContext.getInstance(Options.DEFAULT_SSL_PROTOCOL);
        ctx.init(createTestKeyManagers(), createTestTrustManagers(), new SecureRandom());
        return ctx;
    }
}

public class NatsConnectTLS {
    public static void main(String[] args) {

        try {
            SSLContext ctx = SSLUtils.createSSLContext();
            Options options = new Options.Builder()
                                .server("nats://localhost:4222")
                                .sslContext(ctx) // Set the SSL context
                                .build();
            Connection nc = Nats.connect(options);

            // Do something with the connection

            nc.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
