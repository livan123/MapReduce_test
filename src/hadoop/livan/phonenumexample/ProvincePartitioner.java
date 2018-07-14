package hadoop.livan.phonenumexample;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/* k2��v2��Ӧ����map���kv�����ͣ�
 * �����implements�������չ������������extends����չ��һ����������Լ�
*/
public class ProvincePartitioner 
	extends Partitioner<Text, FlowBean>{

	public static HashMap<String, Integer> provinceDict = 
			new HashMap<String, Integer>();
	static{
		provinceDict.put("138", 0);
		provinceDict.put("139", 1);
		provinceDict.put("136", 2);
		provinceDict.put("137", 3);
	}
	@Override
	public int getPartition(
			Text key, 
			FlowBean value, 
			int numPartitions) {
		String prefix = key.toString().substring(0,3);
		Integer provinceId = provinceDict.get(prefix);
		
		return provinceId==null?4:provinceId;
	}
}
