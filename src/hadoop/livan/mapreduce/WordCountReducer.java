package hadoop.livan.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//keyin,valuein:对应mapper输出的keyout，valueout类型；
//keyout是单词，valueout是总字数；
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	//入参key是一组相同单词kv对的key，<hello,1>……
	//<hello,1><hello,1><hello,1><hello,1><hello,1><hello,1>
	//hello可以只有一个，但是value有很多，因此value传入时用到的是迭代器；
	//一组调一次；即调一次hello就会将所有的hello全部调完；
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) 
					throws IOException, InterruptedException {
		//现在需要将value进行累加；
		int count=0;
		/* 其中一种方法：
		 * Iterator<IntWritable> iterator = values.iterator();
		while(iterator.hasNext()){
			count+=iterator.next().get();
		}*/
		for(IntWritable value:values){
			count+=value.get();
		}
		context.write(key, new IntWritable(count));
		//reduce task默认会写到一个文件中，三个reduce会写到hdfs的三个文件中
		//具体的目录需要进行相应的规定；
		//输入数据的目录也要进行规定；
		//因此需要一个wordcountdriver
	}
}
