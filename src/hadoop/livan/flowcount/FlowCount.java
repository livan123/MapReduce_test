package hadoop.livan.flowcount;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCount {
	
	//map�Ƕ�һ������ԭʼ���ݣ��Զ���һ��bean��
	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		
		@Override
		protected void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			//��һ������ת��string��
			String line = value.toString();
			//�з��ֶ�
			String[] fields = line.split("\t");
			//ȡ���ֻ��ţ�
			String phoneNbr = fields[1];
			//ȡ��������������
			long upFlow = Long.parseLong(fields[fields.length-3]);
			long dFlow = Long.parseLong(fields[fields.length-2]);
			
			context.write(new Text(phoneNbr), new FlowBean(upFlow, dFlow));			
		}
	}
	
	static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		
		//����һ�������������棺
		TreeMap<FlowBean, Text> treeMap = new TreeMap<FlowBean, Text>();
		
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, 
				Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			long sum_upFlow = 0;
			long sum_dFlow = 0;
			//�������е�bean�������е��������������������ֱ��ۼӣ�
			//����������ӦΪ<12939293484, bean>����
			for(FlowBean bean:values){
				sum_upFlow += bean.getdFlow();
				sum_dFlow += bean.getdFlow();
				
			}
			FlowBean resultBean = new FlowBean(sum_upFlow, sum_dFlow);
			treeMap.put(resultBean, key);
//			context.write(key, resultBean);
		}
		
		@Override
		protected void cleanup(Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			Set<Entry<FlowBean, Text>> entrySet = treeMap.entrySet();
			for(Entry<FlowBean, Text> ent:entrySet){
				context.write(ent.getValue(), ent.getKey());
			}
		}
	}

	public static void main(String[] args) throws Exception{
		//��Ϊyarn�����е����ݣ�
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//ָ��jar����ŵ�·������Ҫд����
		//job.setJar("/home/hadoop/wc.jar");
		//��һд���ǰ���main����������·����ֻҪmain����һ������
		//����ͻᶨλ��main�������ڵ�·����
		job.setJarByClass(FlowCount.class);
		//ָ����ҵ��jobҪʹ�õ�mapper/reducerҵ���ࣺ
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		//ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		//ָ��������������ݵ�kv���ͣ�
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
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
