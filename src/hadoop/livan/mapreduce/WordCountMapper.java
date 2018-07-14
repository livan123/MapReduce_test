package hadoop.livan.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * ��Ȼ�˼�MapReduce�ĳ���д���ˣ�������ݡ�д���ݵ��������Ѿ�������ˣ�
 * 	ֻҪ�����ݴ�����װ�������ɡ�
 * keyin:Ĭ���������mr���������һ���ı�����ʼƫ������Long
 *  ��������mr�����Լ��ĸ���������л��ӿڣ����Բ�ֱ����Long��������LongWritable��
 * valuein:��mr�����������һ���ı������ݣ�String,ͬ�ϣ�Text
 * keyout�����û��Զ����߼��������֮����������е�key���ڴ˴��ǵ��ʣ�String��ͬ�ϣ���Text
 * valueout�����û��Զ����߼��������֮����������е�value���ڴ˴��ǵ��ʴ�����Integer��ͬ�ϣ���IntWritable
*/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	/*
	 * ��д�����е�map
	 * map�׶ε�ҵ���߼���д���Զ����map������
	 * maptask���ÿһ���������ݵ���һ�������Զ����map������
	 * ���������ݴ���map:
	 * �������keyΪ
	 * �������valueΪÿһ�е����ݣ�
	 */	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, 
			Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//��maptask�������ǵ�����ת����String;
		String line = value.toString();
		//���ݿո���һ���зֳɵ��ʣ�
		String[] words = line.split(" ");
		//���������Ϊ<key��1>:
		for(String word: words){
			//��������Ϊkey��������1��Ϊvalue�����Ը��ݵ��ʷַ����Ա���ͬ��
			//���ʻᵽ��ͬ��reduce task��ȥ��
			context.write(new Text(word), new IntWritable(1));
			//map task�Ὣ���ݰ���word�Ĳ�ͬ������һ��Ȼ��ͳһ����reduce��
		}
	}
}
