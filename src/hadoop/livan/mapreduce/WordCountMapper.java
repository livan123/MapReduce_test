package hadoop.livan.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * 既然人家MapReduce的程序写好了，则读数据、写数据的能力就已经定义好了，
 * 	只要将数据传到封装中来即可。
 * keyin:默认情况下是mr框架所读到一行文本的起始偏移量，Long
 *  （但是在mr中有自己的更精简的序列化接口，所以不直接用Long，而是用LongWritable）
 * valuein:是mr框架所读到的一行文本的内容，String,同上，Text
 * keyout：是用户自定义逻辑处理完成之后输出数据中的key，在此处是单词，String，同上，用Text
 * valueout：是用户自定义逻辑处理完成之后输出数据中的value，在此处是单词次数，Integer，同上，用IntWritable
*/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	/*
	 * 重写父类中的map
	 * map阶段的业务逻辑就写在自定义的map方法中
	 * maptask会对每一行输入数据调用一次我们自定义的map方法；
	 * 将输入数据传给map:
	 * 输进来的key为
	 * 输进来的value为每一行的数据；
	 */	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, 
			Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//将maptask传给我们的内容转换成String;
		String line = value.toString();
		//根据空格将这一行切分成单词；
		String[] words = line.split(" ");
		//将单词输出为<key，1>:
		for(String word: words){
			//将单词作为key，将次数1作为value，可以根据单词分发，以便相同的
			//单词会到相同的reduce task中去；
			context.write(new Text(word), new IntWritable(1));
			//map task会将内容按照word的不同汇总在一起，然后统一给到reduce；
		}
	}
}
