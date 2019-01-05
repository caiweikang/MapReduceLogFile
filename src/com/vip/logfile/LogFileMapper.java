package com.vip.logfile;

import java.io.IOException;

import javax.security.auth.callback.LanguageCallback;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.vip.entity.LogBean;

public class LogFileMapper extends Mapper<LongWritable, Text, Text, LogBean>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LogBean>.Context context)
			throws IOException, InterruptedException {
		// 1.获取每行输入
		String line = value.toString();
		String[] data= line.split("\\|");
		// 2.获取文件名
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		
		// 3.封装对象
		LogBean logBean = new LogBean();
		logBean.setReportTime(fileName.split("_")[1]);
		logBean.setCellid(data[16]);
		logBean.setAppType(Integer.parseInt(data[22]));
		logBean.setAppSubtype(Integer.parseInt(data[23]));
		logBean.setUserIP(data[26]);
		logBean.setUserPort(Integer.parseInt(data[28]));
		logBean.setAppServerIP(data[30]);
		logBean.setAppServerPort(Integer.parseInt(data[32]));
		logBean.setHost(data[58]);
		
		// 请求的状态码，103表示请求成功
		int appTypeCode = Integer.parseInt(data[18]);
		// 传输状态码
		String transStatus = data[54];
		
		if(logBean.getCellid() == null || logBean.getCellid().equals("")) {
			logBean.setCellid("000000000");
		}
		if(appTypeCode == 103) {
			logBean.setAttempts(1);
		}
		if(appTypeCode == 103 && "10,11,12,13,14,15,32,33,34,35,36,37,38,48,49,50".equals(transStatus)) {
			logBean.setAccepts(1);
		} else {
			logBean.setAccepts(0);
		}
		
		if(appTypeCode == 103) {
			logBean.setTrafficUL(Long.parseLong(data[33]));
		}
		if(appTypeCode == 103) {
			logBean.setTrafficDL(Long.parseLong(data[34]));
		}
		if(appTypeCode == 103) {
			logBean.setRetranUL(Long.parseLong(data[39]));
		}
		if(appTypeCode == 103) {
			logBean.setRetranDL(Long.parseLong(data[40]));
		}
		
		if(appTypeCode == 103) {
			logBean.setTransDelay(Long.parseLong(data[20]) - Long.parseLong(data[19]));
		}
		
		// 唯一用户标识
		String uniqKey = logBean.getCellid() + "|" + logBean.getAppType() + "|" + logBean.getAppSubtype() + "|" + 
		logBean.getUserIP() + "|" + logBean.getUserPort() + "|" + logBean.getAppServerIP() + "|" + 
				logBean.getAppServerPort() + "|" + logBean.getHost() + "|" + logBean.getReportTime();
		
		// 4.传给Reducer
		context.write(new Text(uniqKey), logBean);
		
	}

}
