package hadoop.livan.secondrysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ItemIdPartitioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean bean, NullWritable value, int numpartitions) {
		//��ͬid�Ķ���bean���ᷢ����ͬ��partition
		//���ң���hash���������ķ��������ǻ���û��趨��reduce task������һ�£�
		return (bean.getItemid().hashCode()&Integer.MAX_VALUE) % numpartitions;
		
	
	
	}

}
