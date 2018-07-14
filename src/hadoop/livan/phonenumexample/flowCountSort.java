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
			//�õ�������һ��ͳ�Ƴ�������������Ѿ����ֻ��ŵ���������Ϣ
			String line = value.toString();
			String[] fields = line.split("\t");
			String phoneNbr = fields[0];
			long upFlow = Long.parseLong(fields[1]);
			long dFlow = Long.parseLong(fields[2]);
			bean.set(upFlow, dFlow);
			v.set(phoneNbr);
			//�˴��������л����������Բ��õ����ڴ��ַ���ǣ�
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
	
	//Ȼ���������湹��һ��main������
	
}
