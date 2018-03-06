package ecdlp;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

import scala.Tuple2;

public class SparkECGen {

    public static int TOTAL_POINTS = 10000;
    public static BigInteger p = new BigInteger("6277101735386680763835789423207666416083908700390324961279", 10);
    public static BigInteger b = new BigInteger("64210519e59c80e70fa7e9ab72243049feb8deecc146b9b1", 16);
    public static BigInteger a = new BigInteger("-3", 10);

    public static EllipticCurve ec = new EllipticCurve(a, b, p);

    public static BigInteger px = new BigInteger("188da80eb03090f67cbf20eb43a18800f4ff0afd82ff1012", 16);
    public static BigInteger py = new BigInteger("07192b95ffc8da78631011ed6b24cdd573f977a11e794811", 16);
    public static ECPoint P = new ECPoint(ec, px, py);

    public static BigInteger l = new BigInteger("1234", 10);
    public static ECPoint Q = P.multiply(l);

    public static int distinguished = 160;
    public static int numSplits = 1;
    
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
 
    public static  void close() {
        session.close();
        cluster.close();
    }
    
    
    public static void main(String[] args) {
        LocalDateTime before = LocalDateTime.now();
        SparkConf conf = new SparkConf().setAppName("simpleapp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> l = new ArrayList<>(TOTAL_POINTS);
        for (int i = 0; i < TOTAL_POINTS; i++) {
            l.add(i);
        }
        
        connect("172.16.5.254", null);
        JavaPairRDD<Integer, Iterable<TriTuple>> res = sc.parallelize(l, numSplits).mapToPair(i -> {
            //double x = Math.random();
            //double y = Math.random();
             Random rm = new Random();
             BigInteger a = new BigInteger(160, rm);
             BigInteger b = new BigInteger(160, rm);

             ECPoint X = P.multiply(a);
             X = X.add(Q.multiply(b));
             while (true) {

                 int wt = X.weight();
                 if (wt <= distinguished) {
                     // output key-value pair

                     int ikey = wt % numSplits;

                     // value is (X, a, b)
                     BigIntegerWritable ai = new BigIntegerWritable(a);
                     BigIntegerWritable bi = new BigIntegerWritable(b);

                     TriTuple tt = new TriTuple(X, ai, bi);

                     return new Tuple2<Integer, TriTuple>(ikey, tt);
                 }
                 // iteration to find a distinguished point
                 if (wt % 3 == 0) {

                     X = X.add(Q);
                     b = b.add(BigInteger.ONE);
                 } else if (wt % 3 == 1) {

                     X = X.add(X);
                     a = a.add(BigInteger.ONE);
                     b = b.add(BigInteger.ONE);
                 } else {

                     X = X.add(P);
                     a = a.add(BigInteger.ONE);
                 }
             }
        }).groupByKey().filter(tuple -> {
            int i = 0;
            for(TriTuple tt :tuple._2) {
                String x = tt.getPt().get().getx().toString();
                
                getSession().execute("select count(*) from ecdlp.points where x=?", x);
                
                getSession().execute("insert into ecdlp.points(x,a,b) values (?,?,?)", 
                        x, tt.getAi().get().toString(), tt.getBi().get().toString());
                //i++;
            }
            //if(i>1) System.out.println("Found duplicate");
            
            return i > 0;
        });
        Map<Integer, Iterable<TriTuple>> duplicates = res.collectAsMap();
        
        res.saveAsTextFile("output/cuspark2");
        //System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
        LocalDateTime after = LocalDateTime.now();
        long time = ChronoUnit.MILLIS.between(before, after);
        System.out.println("Total execution time is " + time);
        close();
        
    }
}
