package hadoop.livan.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//keyin,valuein:��Ӧmapper�����keyout��valueout���ͣ�
//keyout�ǵ��ʣ�valueout����������
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	//���key��һ����ͬ����kv�Ե�key��<hello,1>����
	//<hello,1><hello,1><hello,1><hello,1><hello,1><hello,1>
	//hello����ֻ��һ��������value�кܶ࣬���value����ʱ�õ����ǵ�������
	//һ���һ�Σ�����һ��hello�ͻὫ���е�helloȫ�����ꣻ
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) 
					throws IOException, InterruptedException {
		//������Ҫ��value�����ۼӣ�
		int count=0;
		/* ����һ�ַ�����
		 * Iterator<IntWritable> iterator = values.iterator();
		while(iterator.hasNext()){
			count+=iterator.next().get();
		}*/
		for(IntWritable value:values){
			count+=value.get();
		}
		context.write(key, new IntWritable(count));
		//reduce taskĬ�ϻ�д��һ���ļ��У�����reduce��д��hdfs�������ļ���
		//�����Ŀ¼��Ҫ������Ӧ�Ĺ涨��
		//�������ݵ�Ŀ¼ҲҪ���й涨��
		//�����Ҫһ��wordcountdriver
	}
}
