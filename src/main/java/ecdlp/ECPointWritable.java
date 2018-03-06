package ecdlp;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;

import org.apache.hadoop.io.WritableComparable;

public class ECPointWritable implements WritableComparable<ECPointWritable>, Serializable {
	
	private ECPoint ecPoint;
	
	public static BigInteger p = new BigInteger("6277101735386680763835789423207666416083908700390324961279", 10);
	public static BigInteger b = new BigInteger("64210519e59c80e70fa7e9ab72243049feb8deecc146b9b1", 16);
	public static BigInteger a = new BigInteger("-3", 10);

	public static EllipticCurve ec = new EllipticCurve(a, b, p);
	
	public ECPointWritable(ECPoint p) {
		ecPoint = p;
	}
	
	public ECPointWritable() {
		ecPoint = new ECPoint(ec);
	}
	
	public ECPoint get() {
		return ecPoint;
	}
	
	public void set(ECPoint p) {
		ecPoint = p;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		BigIntegerWritable x = new BigIntegerWritable();
		BigIntegerWritable y = new BigIntegerWritable();
		x.readFields(arg0);
		y.readFields(arg0);
		ecPoint = new ECPoint(ec, x.get(), y.get());
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		BigIntegerWritable x = new BigIntegerWritable(ecPoint.getx());
		BigIntegerWritable y = new BigIntegerWritable(ecPoint.gety());
		x.write(arg0);
		y.write(arg0);
		
	}

	@Override
	public int compareTo(ECPointWritable o) {
		if(this.ecPoint.getx().equals(o.ecPoint.getx()) &&
				this.ecPoint.gety().equals(o.ecPoint.gety()))
			return 0;
		else
			return -1;
	}
	
	public String toString() {
	    return "[X=" + ecPoint.getx() + ", Y=" + ecPoint.gety() + "]";
	}



}
