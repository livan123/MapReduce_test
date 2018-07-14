package hadoop.livan.phonenumexample;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class flowCountSort {
	static class FlowCountSortMapper extends 
		Mapper<LongWritable, Text, FlowBean, Text>{
		FlowBean bean = new FlowBean();
		Text v = new Text();
		@Override
		protected void map(
				LongWritable key, 
				Text value, 
				Context context)
				throws IOException, InterruptedException {
			//拿到的是上一个统计程序的输出结果，已经是手机号的总流量信息
			String line = value.toString();
			String[] fields = line.split("\t");
			String phoneNbr = fields[0];
			long upFlow = Long.parseLong(fields[1]);
			long dFlow = Long.parseLong(fields[2]);
			bean.set(upFlow, dFlow);
			v.set(phoneNbr);
			//此处会有序列化操作，所以不用担心内存地址覆盖；
			context.write(bean, v);
		}
	}
	static class FlowCountSortReducer 
		extends Reducer<FlowBean, Text, Text, FlowBean>{
		//<bean(), phonenbr><bean(), phonenbr>
		@Override
		protected void reduce(
				FlowBean bean, 
				Iterable<Text> values, 
				Reducer<FlowBean, Text, 
				Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			context.write(values.iterator().next(), bean);
		}
	}
	
	//然后再在下面构建一个main函数：
	
}
