package hadoop.livan.flowcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
	//bean中封装上行流量、下行流量；
	private long upFlow;
	private long dFlow;
	private long sumFlow;
	//反序列化时需要反射调用空参序列函数；
	public FlowBean(){}
	public FlowBean(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.setSumFlow(upFlow + dFlow);
	}
	
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getdFlow() {
		return dFlow;
	}
	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}
	@Override
	public String toString() {
		return upFlow+"\t"+dFlow+"\t"+sumFlow;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		//反序列化对象的时候需要用到
		long upFlow = in.readLong();
		//从序列化内容中拿出一些内容
		long dFlow = in.readLong();
		long sumFlow = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//序列化对象的时候需要用到
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
		//序列化的顺序，与反序列化的顺序完全一致；
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}
	
	
}
