package hadoop.livan.QQfriendlink;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SharedFriendsStepTwo {
	
	static class SharedFriendsStepTwoMapper extends
	Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//�õ�����������һ�������������;
			//A:I,K,L,J����
			//�ѣ��ˣ��ˣ��ˣ��ˡ���
			String line = value.toString();
			String[] friend_persons = line.split("\t");
			
			String friend = friend_persons[0];
			String[] persons = friend_persons[1].split(",");
			
			Arrays.sort(persons);
			for(int i=0; i<persons.length-2; i++){
				for(int j=i+1; j<persons.length-1;j++){
					//����<��-�ˣ� ����>����������ͬ�ġ���-�ˡ��Ե����к��Ѿͻᵽͬһ��reduce��
					context.write(new Text(persons[i]+"-"+persons[j]), 
							new Text(friend));
				}
			}
			
		}
	}
	
	static class SharedFriendsStepTwoReducer 
	extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text person_person, Iterable<Text> friends, 
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text friend:friends){
				sb.append(friend).append(" ");
			}
			context.write(person_person, new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(SharedFriendsStepTwo.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(SharedFriendsStepTwoMapper.class);
		job.setReducerClass(SharedFriendsStepTwoReducer.class);
		
		FileInputFormat.setInputPaths(job, 
				new Path("c:/wordcount/friend-step2"));
		FileOutputFormat.setOutputPath(job, 
				new Path("c:/wordcount/friend-step3"));
		
		job.waitForCompletion(true);
	}
}
