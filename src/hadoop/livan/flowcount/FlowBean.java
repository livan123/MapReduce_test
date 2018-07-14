package hadoop.livan.flowcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
	//bean�з�װ��������������������
	private long upFlow;
	private long dFlow;
	private long sumFlow;
	//�����л�ʱ��Ҫ������ÿղ����к�����
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
		//�����л������ʱ����Ҫ�õ�
		long upFlow = in.readLong();
		//�����л��������ó�һЩ����
		long dFlow = in.readLong();
		long sumFlow = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//���л������ʱ����Ҫ�õ�
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
		//���л���˳���뷴���л���˳����ȫһ�£�
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}
	
	
}
