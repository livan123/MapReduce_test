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
	 * 相当于一个yarn集群的客户端；
	 * 需要封装我们mr程序的相关运行参数，指定jar包，最后提交给yarn
	*/
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//指定jar包存放的路径，不要写死；
		//job.setJar("/home/hadoop/wc.jar");
		//这一写法是按照main方法的启动路径，只要main方法一启动，
		//程序就会定位到main方法所在的路径；
		job.setJarByClass(WordCountDriver.class);
		//指定本业务job要使用的mapper/reducer业务类：
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//指定需要用哪个combiner，以及用哪个类作为combiner的逻辑
		job.setCombinerClass(WordCountReducer.class);
		
		//指定最终输出的数据的kv类型：
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
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
