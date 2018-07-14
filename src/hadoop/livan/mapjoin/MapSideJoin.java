package hadoop.livan.mapjoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapSideJoin {

	static class MapSideJoinMapper extends 
		Mapper<LongWritable, Text, Text, NullWritable>{
		//������Ļ����ļ��Ѿ�ָ���ã�map�оͿ���ֱ��ʹ�ã�
		
		//��һ��hashmap�����ر����Ʒ��Ϣ��
		Map<String, String> pdInfoMap = new HashMap<String, String>();
		Text k = new Text();
		
		
		/*ͨ���Ķ�����mapper��Դ�룬���֣�setup����
		����maptask��������֮ǰ����һ�Σ�
		����������һЩ��ʼ������*/
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, 
				NullWritable>.Context context)
				throws IOException, InterruptedException {
			BufferedReader br = new BufferedReader(
					new InputStreamReader(new FileInputStream("pdts.txt")));
			String line;
			while(StringUtils.isNotEmpty(line=br.readLine())){
				String[] fields = line.split(",");
				pdInfoMap.put(fields[0], fields[1]);
			}
			br.close();
		}
		
		@Override
		protected void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String orderLine = value.toString();
			String[] fields = orderLine.split("\t");
			String pdName = pdInfoMap.get(fields[1]);
			k.set(orderLine+"\t"+pdName);
			context.write(k, NullWritable.get());
		}
	}
	
	public static void main(String[] args) 
			throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MapSideJoin.class);
		job.setMapperClass(MapSideJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("C:/wordcount/mapjoininput"));
		FileOutputFormat.setOutputPath(job, new Path("C:/wordcount/mapjoinoutput"));
		
		//ָ����Ҫ����һ���ļ������е�maptask���нڵ㹤��Ŀ¼
		/*job.addArchiveToClassPath(archive);*/
		/*����jar����task���нڵ��classpath��*/
		
		/*job.addCacheFile(uri); *//*������ͨ�ļ���task���нڵ�Ĺ���Ŀ¼��*/
		/*job.addFileToClassPath(file);*/ /*������ͨ�ļ���task���нڵ�Ĺ���Ŀ¼��*/
		
		/*�˳�����Ȼֻд��һ��map��������һ��Ĭ�ϵ�reduce����ʱ��Ҫȥ��reduce��*/
		job.setNumReduceTasks(0);
		
		//����Ʒ�ļ����浽task�����ڵ�Ĺ���Ŀ¼��
		/*������ͨ�ļ���task���нڵ�Ĺ���Ŀ¼*/
		job.addCacheFile(new URI("file:/C:/wordcount/mapjoincache/pdts.txt"));  
		
		Boolean res = job.waitForCompletion(true);
		System.out.println(res?0:1);
		
	}
}
