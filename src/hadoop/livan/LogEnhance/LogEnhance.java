package hadoop.livan.LogEnhance;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogEnhance {
	
	static class LogEnhanceMapper extends Mapper<LongWritable, 
													Text, Text, NullWritable>{
		Map<String, String> ruleMap = new HashMap<String, String>();
		
		Text k = new Text();
		NullWritable v =NullWritable.get();
		
		@Override
		protected void setup(Mapper<LongWritable, 
				Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			try {
				DBLoader.dbLoader(ruleMap);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//��ȡһ������������¼���Ϸ�����־������
			Counter counter = context.getCounter("malformed","malformedline");
			
			String line = value.toString();
			String[] fields = StringUtils.split(line, "\t");
			String url = fields[26];
			String content_tag = ruleMap.get(url);
			//�ж�֪ʶ���ݱ�ǩ�Ƿ�Ϊ�գ���ֻ���url�������嵥
			//�����ֵ�����������ǿ��־��
			if(content_tag ==null){
				k.set(url);
				context.write(k, v);
			}else{
				k.set(line + "\t" + content_tag + "\n");
				context.write(k, v);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(LogEnhance.class);
		job.setMapperClass(LogEnhanceMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("C:/wordcount/mapjoininput"));
		
		//�Զ���һ��output�࣬����������ݵĲ�ͬд����ͬ�ĵط�ȥ
		FileOutputFormat.setOutputPath(job, new Path("C:/wordcount/mapjoinoutput"));
		
		/*�˳�����Ȼֻд��һ��map��������һ��Ĭ�ϵ�reduce����ʱ��Ҫȥ��reduce��*/
		job.setNumReduceTasks(0);
		
		Boolean res = job.waitForCompletion(true);
		System.out.println(res?0:1);
	}
}
