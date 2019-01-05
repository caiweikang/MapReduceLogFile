package com.vip.logfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.vip.entity.LogBean;

public class LogFileDriver {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		
		// 1.����Job������������
		job.setJarByClass(LogFileDriver.class);
		// ����Mapper������е���
		job.setMapperClass(LogFileMapper.class);
		// ����Reducer���������
		job.setReducerClass(LogFileReducer.class);
		
		// 2.����Mapper������key������
		job.setMapOutputKeyClass(Text.class);
		// ����Mapper������value����
		job.setMapOutputValueClass(LogBean.class);
		// ����Reducer������key������
		job.setOutputKeyClass(LogBean.class);
		// ����Reducer������value����
		job.setOutputValueClass(NullWritable.class);
		
		// ����Reducer�����Լ��Զ����������
//		job.setNumReduceTasks(3);
//		job.setPartitionerClass(FlowPartition.class);
		
		// 3.����job�������ļ����ڵ�HDFS·��
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.154.129:9000/logFile/103_20150615143630_00_00_000_2.csv"));
		// ���ý���ļ�·����Ҳ�����Ǵ洢HDFS��
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.154.129:9000/logFile/result"));
		
		// 4.�ύ����
		job.waitForCompletion(true);


	}

}
