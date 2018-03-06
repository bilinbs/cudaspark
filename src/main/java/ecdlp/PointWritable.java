package ecdlp;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import sun.util.logging.resources.logging;

public class PointWritable implements Writable, Serializable{
	private int a;
	private int b;
	
	public PointWritable(int a, int b) {
		this.a = a;
		this.b = b;
	}
	
	public PointWritable(PointWritable p) {
		a = p.getA();
		b = p.getB();
	}
	
	public int getA() {
		return a;
	}

	public void setA(int a) {
		this.a = a;
	}

	public int getB() {
		return b;
	}

	public void setB(int b) {
		this.b = b;
	}
	
	public void print() {
		System.out.println(a);
	}
	
	public static PointWritable add(PointWritable lh, PointWritable rh) {
		return new PointWritable(lh.a+rh.a, lh.b+rh.b);
	}
	
	public static PointWritable scalar(int k, PointWritable p) {
		return new PointWritable(p);
	}
	
	public static PointWritable random_initial() {
		return new PointWritable(0, 0);
	}
	
	public int subset() {
		return 1;
	}
	
	public boolean isEqualTo(PointWritable pw) {
		return pw.a == this.a && pw.b == this.b;
	}
	
	public boolean distinguished() {
		return true;
	}
	
	public int key() {
		return 1;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		IntWritable a = new IntWritable();
		IntWritable b = new IntWritable();
		a.readFields(arg0);
		b.readFields(arg0);
		this.a = a.get();
		this.b = b.get();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		IntWritable a = new IntWritable();
		IntWritable b = new IntWritable();
		a.set(this.a);
		b.set(this.b);
		a.write(arg0);
		b.write(arg0);
	}

	
}
