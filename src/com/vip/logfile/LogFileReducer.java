package com.vip.logfile;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.inject.Key;
import com.vip.entity.LogBean;

public class LogFileReducer extends Reducer<Text, LogBean, LogBean, NullWritable>{
	@Override
	protected void reduce(Text uniqKey, Iterable<LogBean> logBeans, Reducer<Text, LogBean, LogBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		LogBean oldBean = new LogBean();
		String[] data = uniqKey.toString().split("\\|");
		oldBean.setCellid(data[0]);
		oldBean.setAppType(Integer.parseInt(data[1]));
		oldBean.setAppSubtype(Integer.parseInt(data[2]));
		oldBean.setUserIP(data[3]);
		oldBean.setUserPort(Integer.parseInt(data[4]));
		oldBean.setAppServerIP(data[5]);
		oldBean.setAppServerPort(Integer.parseInt(data[6]));
		oldBean.setHost(data[7]);
		oldBean.setReportTime(data[8]);
		
		for(LogBean newBean : logBeans) {
			oldBean.setAccepts(oldBean.getAccepts() + newBean.getAccepts());
			oldBean.setAttempts(oldBean.getAttempts() + newBean.getAttempts());
			oldBean.setTrafficDL(oldBean.getTrafficDL() + newBean.getTrafficDL());
			oldBean.setTrafficUL(oldBean.getRetranUL() + newBean.getTrafficUL());
			oldBean.setRetranDL(oldBean.getRetranDL()+ newBean.getRetranDL());
			oldBean.setRetranUL(oldBean.getRetranUL() + newBean.getRetranUL());
			oldBean.setTransDelay(oldBean.getTransDelay() + newBean.getTransDelay());
		}
		context.write(oldBean, NullWritable.get());
	}
}
