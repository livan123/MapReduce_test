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

/*maptask或者reducetask在最终输出时，先调用outputformat
 * 的getRecordWriter方法拿到一个RecordWriter
然后再调用recordwriter的write(k, v)方法将数据写出；*/

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

	
	//构造一个自己的记录
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
			//如果要写出的数据是待爬的url，则写入待爬清单文件url.dat
			if(result.contains("tocrawl")){
				tocrawlOs.write(result.getBytes());
			}else{
				//如果要写出的数据是增强日志，则写出的是增强日志文件
				enhancedOs.write(result.getBytes());
			}
		}
	}
}
