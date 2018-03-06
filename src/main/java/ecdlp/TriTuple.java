package ecdlp;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;


public class TriTuple implements WritableComparable<TriTuple>, Serializable {

	private ECPointWritable pt;
	private BigIntegerWritable ai;
	private BigIntegerWritable bi;
	
	public TriTuple(ECPoint tpt, BigIntegerWritable tai, BigIntegerWritable tbi) {
		pt = new ECPointWritable(tpt);
		setAi(tai);
		setBi(tbi);
	}
	
	public TriTuple() {
		pt = new ECPointWritable();
		ai = new BigIntegerWritable();
		bi = new BigIntegerWritable();
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		pt.readFields(arg0);
		ai.readFields(arg0);
		bi.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		pt.write(arg0);
		ai.write(arg0);
		bi.write(arg0);
	}
	
	public boolean isEqualTo(TriTuple tt) {
		return pt.compareTo(tt.pt) == 0 && 
				this.ai.get() == tt.ai.get() && 
				this.bi.get() == tt.bi.get();
	}

	public ECPointWritable getPt() {
		return pt;
	}

	public void setPt(ECPointWritable pt) {
		this.pt = pt;
	}

	public BigIntegerWritable getAi() {
		return ai;
	}

	public void setAi(BigIntegerWritable ai) {
		this.ai = ai;
	}

	public BigIntegerWritable getBi() {
		return bi;
	}

	public void setBi(BigIntegerWritable bi) {
		this.bi = bi;
	}

	@Override
	public int compareTo(TriTuple o) {
		// TODO Auto-generated method stub
		return 0;
	}

    @Override
    public String toString() {
        return "TriTuple [pt=" + pt + ", ai=" + ai + ", bi=" + bi + "]";
    }

	


}
