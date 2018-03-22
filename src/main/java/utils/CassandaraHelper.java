package utils;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

public class CassandaraHelper {
    
    private static CassandaraHelper instance;

    private Cluster cluster;
    private Session session;

    public void connect(String node, Integer port) {
        Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();
    
        session = cluster.connect();
    }

    public Session getSession() {
        return session;
    }

    public void close() {
        session.close();
        cluster.close();
    }

    private CassandaraHelper() {
        super();
    }
    
    public static CassandaraHelper getInstance() {
        if(instance == null) {
            instance = new CassandaraHelper();
        }
        return instance;
    }

}