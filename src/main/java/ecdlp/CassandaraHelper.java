package ecdlp;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

public class CassandaraHelper {

    private static Cluster cluster;
    private static Session session;

    public static void connect(String node, Integer port) {
        Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();
    
        session = cluster.connect();
    }

    public static Session getSession() {
        return session;
    }

    public static void close() {
        session.close();
        cluster.close();
    }

    public CassandaraHelper() {
        super();
    }

}