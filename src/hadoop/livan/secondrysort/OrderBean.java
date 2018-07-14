package hadoop.livan.secondrysort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class OrderBean {

	private Text itemid;
	
	private DoubleWritable amount;
	
	public OrderBean(){
		
	}
	
	public OrderBean(Text itemid, DoubleWritable amount){
		set(itemid, amount);
	}
	
	private void set(Text itemid, DoubleWritable amount) {
		this.itemid = itemid;
		this.amount = amount;
	}

	public Text getItemid() {
		return itemid;
	}
	public void setItemid(Text itemid) {
		this.itemid = itemid;
	}
	public DoubleWritable getAmount() {
		return amount;
	}
	public void setAmount(DoubleWritable amount) {
		this.amount = amount;
	}

}
