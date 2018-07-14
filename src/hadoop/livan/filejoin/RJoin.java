package hadoop.livan.filejoin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RJoin {
	
	static class RJoinMapper extends Mapper<LongWritable, 
		Text, Text, InfoBean>{
		InfoBean bean = new InfoBean();
		
		Text k = new Text();
		
		@Override
		protected void map(LongWritable key, 
				Text value, 
				Mapper<LongWritable, Text, Text, InfoBean>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String name = inputSplit.getPath().getName();
			
			String pid = "";
			
			if(name.startsWith("order")){
				//通过文件名判断是哪种数据
				String[] fields = line.split(",");
				pid = fields[2];
				bean.set(Integer.parseInt(fields[0]), 
						fields[1], 
						pid, 
						Integer.parseInt(fields[3]),
						"", 
						0, 
						0, 
						"0");
			}else{
				//通过文件名判断是哪种数据
				String[] fields = line.split(",");
				pid = fields[0];
				bean.set(0,"",pid,0,fields[1], 
						Integer.parseInt(fields[2]), 
						Float.parseFloat(fields[3]), "1");
			}
			k.set(pid);
			context.write(k, bean);
		}
	}
	
	
	static class RJionReducer extends Reducer<Text, InfoBean, 
											InfoBean, NullWritable>{
		@Override
		protected void reduce(Text pid, Iterable<InfoBean> beans,
				Reducer<Text, InfoBean, InfoBean, NullWritable>.Context context) 
						throws IOException, InterruptedException {
			
			InfoBean pdBean = new InfoBean();
			ArrayList<InfoBean> orderBeans = new ArrayList<InfoBean>();
			
			for(InfoBean bean:beans){
				if("1".equals(bean.getFlag())){
					try {
						BeanUtils.copyProperties(pdBean, bean);
					} catch (Exception e){
						e.printStackTrace();
					}
				}else{
					InfoBean odbean = new InfoBean();
					try {
						BeanUtils.copyProperties(odbean, bean);
					} catch (Exception e) {
						e.printStackTrace();
					}
					orderBeans.add(bean);
				}
			}
			
			//拼接两类数据形成最终结果：
			for(InfoBean bean:orderBeans){
				bean.setPname(pdBean.getPname());
				bean.setCategory_id(pdBean.getCategory_id());
				bean.setPrice(pdBean.getPrice());
				
				context.write(bean, NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) {
		//配置yarn流程
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
