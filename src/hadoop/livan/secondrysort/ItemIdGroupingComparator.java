package hadoop.livan.secondrysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ItemIdGroupingComparator 
	extends WritableComparator{

	//������Ϊkey��bean��class���ͣ��Լ���
	//����Ҫ�ÿ���������ȡʵ������
	@SuppressWarnings("unchecked")
	protected ItemIdGroupingComparator(){
		super((Class<? extends WritableComparable>) OrderBean.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, 
			WritableComparable b) {
		OrderBean abean = (OrderBean) a;
		OrderBean bbean = (OrderBean) b;
		//�Ƚ�����beanʱ��ָ��ֻ�Ƚ�bean�е�orderid��
		return abean.getItemid().
				compareTo(bbean.getItemid());
	}
}
