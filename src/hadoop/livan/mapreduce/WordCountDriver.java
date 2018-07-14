package hadoop.livan.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
	/*
	 * �൱��һ��yarn��Ⱥ�Ŀͻ��ˣ�
	 * ��Ҫ��װ����mr�����������в�����ָ��jar��������ύ��yarn
	*/
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//ָ��jar����ŵ�·������Ҫд����
		//job.setJar("/home/hadoop/wc.jar");
		//��һд���ǰ���main����������·����ֻҪmain����һ������
		//����ͻᶨλ��main�������ڵ�·����
		job.setJarByClass(WordCountDriver.class);
		//ָ����ҵ��jobҪʹ�õ�mapper/reducerҵ���ࣺ
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		//ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//ָ����Ҫ���ĸ�combiner���Լ����ĸ�����Ϊcombiner���߼�
		job.setCombinerClass(WordCountReducer.class);
		
		//ָ��������������ݵ�kv���ͣ�
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//ָ��job������ԭʼ�ļ�����Ŀ¼��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//ָ��job������������Ŀ¼��
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//��job�е���ز����Լ�java������jar���ύ��yarn����
		//job.submit();
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
