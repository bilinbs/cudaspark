package ecdlp;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import scala.Tuple2;

import utils.CassandaraHelper;
import static ecdlp.TABLES.*;

public class SparkECGen  {

    public static final String POINTCOUNT = "ptcount";
    private static final String DOT = ".";
    private static final String COMMA = ",";
    public static int TOTAL_POINTS = 25000;
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
    public static int numSplits = 32;
    
    public static CassandaraHelper db = CassandaraHelper.getInstance();
    public static Session session; 
    public static void main(String[] args) {
        LocalDateTime before = LocalDateTime.now();
        SparkConf conf = new SparkConf().setAppName("sparkecdlp");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> l = new ArrayList<>(TOTAL_POINTS);
        for (int i = 0; i < TOTAL_POINTS; i++) {
            l.add(i);
        }
        
        db.connect("172.16.5.254", null);
        session = db.getSession();
        JavaPairRDD<BigInteger, Iterable<TriTuple>> res = sc.parallelize(l, numSplits)
                .mapToPair(i -> generate(i))
                .groupByKey()
                .filter(tuple -> analyze(tuple));
        Map<BigInteger, Iterable<TriTuple>> duplicates = res.collectAsMap();
        
        res.saveAsTextFile("output/cuspark2");
        //System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
        LocalDateTime after = LocalDateTime.now();
        long time = ChronoUnit.MILLIS.between(before, after);
        System.out.println("Total execution time to compute with "+ TOTAL_POINTS +" points is " + time);
        sc.close();
        db.close();
        
    }

    private static Tuple2<BigInteger, TriTuple> generate(int i) {
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

                     return new Tuple2<BigInteger, TriTuple>(X.getx(), tt);
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
    }

    private static Boolean analyze(Tuple2<BigInteger, Iterable<TriTuple>> tuple) {
        long count = 0;
        
        for(TriTuple tt :tuple._2) {
            String x = tt.getPt().get().getx().toString();
            if(session == null) {
                db.connect("172.16.5.254", null);
                session = db.getSession();
            }
            ResultSet rs = session.execute("select count(*) as " +
                    POINTCOUNT + " from " + KEYSPACE + DOT + POINTS.NAME  + " where " +
                    POINTS.X + "=?", x);
            if(!rs.isExhausted()) {
                Row row = rs.one();
                count = row.getLong(POINTCOUNT);
            }
            if(count > 0) {
                rs = db.getSession().execute("select " + POINTS.X + COMMA + POINTS.A + COMMA + POINTS.B +
                        " from " + KEYSPACE + DOT + POINTS.NAME  + " where " +
                        POINTS.X + "=?", x);
                if(!rs.isExhausted()) {
                    Row row = rs.one();
                    String dA = row.getString(POINTS.A);
                    String dB = row.getString(POINTS.B);
                }
            }
            
            session.execute("insert into ecdlp.points(x,a,b) values (?,?,?)", 
                    x, tt.getAi().get().toString(), tt.getBi().get().toString());
            //i++;
        }
        //if(i>1) System.out.println("Found duplicate");
        
        return count > 0;
    }
}
