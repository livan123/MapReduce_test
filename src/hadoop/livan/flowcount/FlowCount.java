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
	
	//map是读一进来的原始数据，自定义一个bean：
	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		
		@Override
		protected void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			//将一行内容转成string；
			String line = value.toString();
			//切分字段
			String[] fields = line.split("\t");
			//取出手机号：
			String phoneNbr = fields[1];
			//取出上下行流量：
			long upFlow = Long.parseLong(fields[fields.length-3]);
			long dFlow = Long.parseLong(fields[fields.length-2]);
			
			context.write(new Text(phoneNbr), new FlowBean(upFlow, dFlow));			
		}
	}
	
	static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		
		//定义一个容器，即缓存：
		TreeMap<FlowBean, Text> treeMap = new TreeMap<FlowBean, Text>();
		
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, 
				Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			long sum_upFlow = 0;
			long sum_dFlow = 0;
			//遍历所有的bean，将其中的上行流量，下行流量分别累加；
			//进来的数据应为<12939293484, bean>……
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
		//即为yarn中运行的内容：
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//指定jar包存放的路径，不要写死；
		//job.setJar("/home/hadoop/wc.jar");
		//这一写法是按照main方法的启动路径，只要main方法一启动，
		//程序就会定位到main方法所在的路径；
		job.setJarByClass(FlowCount.class);
		//指定本业务job要使用的mapper/reducer业务类：
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		//指定最终输出的数据的kv类型：
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		//指定job的输入原始文件所在目录：
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//指定job的输出结果所在目录：
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//将job中的相关参数以及java类所在jar包提交给yarn运行
		//job.submit();
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);

	}

}
