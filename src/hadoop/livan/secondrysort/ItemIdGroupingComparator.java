package hadoop.livan.secondrysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ItemIdGroupingComparator 
	extends WritableComparator{

	//传入作为key的bean的class类型，以及制
	//定需要让框架做反射获取实例对象；
	@SuppressWarnings("unchecked")
	protected ItemIdGroupingComparator(){
		super((Class<? extends WritableComparable>) OrderBean.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, 
			WritableComparable b) {
		OrderBean abean = (OrderBean) a;
		OrderBean bbean = (OrderBean) b;
		//比较两个bean时，指定只比较bean中的orderid；
		return abean.getItemid().
				compareTo(bbean.getItemid());
	}
}
