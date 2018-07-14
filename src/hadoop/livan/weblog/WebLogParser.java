package hadoop.livan.weblog;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class WebLogParser {
	
	static SimpleDateFormat sd1 = 
			new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	static SimpleDateFormat sd2 = 
			new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static WebLogBean parser(String line){
		WebLogBean webLogBean = new WebLogBean();
		String[] arr = line.split(" ");
		if(arr.length>11){
			webLogBean.setRemote_addr(arr[0]);
			webLogBean.setRemote_user(arr[1]);
			webLogBean.setTime_local(arr[3].substring(1));
			webLogBean.setRequest(arr[6]);
			webLogBean.setStatus(arr[8]);
			webLogBean.setBody_bytes_sent(arr[9]);
			webLogBean.setHttp_referer(arr[10]);
			
			if(arr.length>12){
				webLogBean.setHttp_user_agent(arr[11]+" "+arr[12]);
			}else{
				webLogBean.setHttp_user_agent(arr[11]);
			}
			if(Integer.parseInt(webLogBean.getStatus())>=400){
				//如果大于400，http错误，则设置为false
				webLogBean.setValid(false);
			}
		}else{
			webLogBean.setValid(false);
		}
		return webLogBean;
	}
	
	public static String parserTime(String time){
		time.replace("/", "-");
		return time;
	}
	
	public String parseTime(String dt){
		String timeString = "";
		try {
			Date parse = sd1.parse(dt);
			timeString = sd2.format(parse);
		} catch (ParseException e) {
			
			e.printStackTrace();
		}
		return timeString;
	}
	
	/*//用于测试本类是否正常
	public static void main(String[] args) {
		WebLogParser wp = new WebLogParser();
		String parseTime = wp.parseTime("18/Sep/2013:06:49:48");
		System.out.println(parseTime);
	}*/
}
