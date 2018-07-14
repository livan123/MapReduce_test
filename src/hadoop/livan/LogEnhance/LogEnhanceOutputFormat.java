package hadoop.livan.LogEnhance;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*maptask����reducetask���������ʱ���ȵ���outputformat
 * ��getRecordWriter�����õ�һ��RecordWriter
Ȼ���ٵ���recordwriter��write(k, v)����������д����*/

public class LogEnhanceOutputFormat 
		extends FileOutputFormat<Text, NullWritable>{

	@Override
	public RecordWriter<Text, NullWritable> 
					getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSystem fs = FileSystem.get(context.getConfiguration());
		
		Path enhancePath = new 
				Path("hdfs://hdp-node-01:9000/logEnhance/enhancelog/log.dat");
		Path tocrawlPath = new 
				Path("hdfs://hdp-node-01:9000/logEnhance/tocrawl/url.dat");
		
		FSDataOutputStream enhancedOs = fs.create(enhancePath);
		FSDataOutputStream tocrawlOs = fs.create(tocrawlPath);
		
		return new EnhanceRecordWriter<Text, NullWritable>(
				enhancedOs, tocrawlOs);
		
	}

	
	//����һ���Լ��ļ�¼
	static class EnhanceRecordWriter<Text, NullWritable> 
									extends RecordWriter<Text, NullWritable>{

		FSDataOutputStream enhancedOs = null;
		FSDataOutputStream tocrawlOs = null;
		
		public EnhanceRecordWriter(FSDataOutputStream enhancedOs, 
				FSDataOutputStream tocrawlOs) {
			super();
			this.enhancedOs = enhancedOs;
			this.tocrawlOs = tocrawlOs;
		}

		@Override
		public void close(TaskAttemptContext context) 
				throws IOException, InterruptedException {
			if(tocrawlOs!=null){
				tocrawlOs.close();
			}
			if(enhancedOs!=null){
				enhancedOs.close();
			}
		}

		@Override
		public void write(Text key, NullWritable value) 
				throws IOException, InterruptedException {
			String result = key.toString();
			//���Ҫд���������Ǵ�����url����д������嵥�ļ�url.dat
			if(result.contains("tocrawl")){
				tocrawlOs.write(result.getBytes());
			}else{
				//���Ҫд������������ǿ��־����д��������ǿ��־�ļ�
				enhancedOs.write(result.getBytes());
			}
		}
	}
}
