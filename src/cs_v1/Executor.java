package cs_v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Executor {
	
	private static String file = "TestExperimentData10";
	public static void main(String[] args) throws Exception {
		
		long startTime = System.currentTimeMillis();//获取当前时间
		Configuration conf = new Configuration();
		
		FileSystem fs =FileSystem.get(conf);
		if(fs.exists(new Path("CommunitySearch/"+file+"/"+file+"Index"))) {
			fs.delete(new Path("CommunitySearch/"+file+"/"+file+"Index"),true);
		}
		
		//建立倒排索引的mapreduce
		Job BuildInvertedIndex = new Job();
		BuildInvertedIndex.setJarByClass(Executor.class);
		BuildInvertedIndex.setJobName("BuildInvertedIndex");
		
		FileInputFormat.addInputPath(BuildInvertedIndex, new Path("cpmOutput/cpm"+file+"Output/finalResult/FinalClique"));
		FileOutputFormat.setOutputPath(BuildInvertedIndex, new Path("CommunitySearch/"+file+"/"+file+"Index"));
		
		BuildInvertedIndex.setMapperClass(InvertedIndex_Mapper.class);
		BuildInvertedIndex.setReducerClass(InvertedIndex_Reducer.class);
		
		BuildInvertedIndex.setOutputKeyClass(Text.class);
		BuildInvertedIndex.setOutputValueClass(Text.class);
		
		BuildInvertedIndex.waitForCompletion(true);
		
		if(fs.exists(new Path("CommunitySearch/"+file+"/"+file+"CommunitySearch"))) {
			fs.delete(new Path("CommunitySearch/"+file+"/"+file+"CommunitySearch"),true);
		}
		
		//进行社区查找的mapreduce
		Job CommunitySearch = new Job();
		CommunitySearch.setJarByClass(Executor.class);
		CommunitySearch.setJobName("CommunitySearch");
		
		FileInputFormat.addInputPath(CommunitySearch, new Path("CommunitySearch/"+file+"/"+file+"Index"));
		FileOutputFormat.setOutputPath(CommunitySearch, new Path("CommunitySearch/"+file+"/"+file+"CommunitySearch"));
		
		CommunitySearch.setMapperClass(CS_Mapper.class);
		CommunitySearch.setReducerClass(CS_Reducer.class);
		
		CommunitySearch.setOutputKeyClass(Text.class);
		CommunitySearch.setOutputValueClass(Text.class);
		
		CommunitySearch.waitForCompletion(true);
		long endTime = System.currentTimeMillis();
		HandleData h = new HandleData();
		String content = "查询的文件为: "+file+"\n"+"程序运行总时间:"+(double)(endTime-startTime)/1000 + "秒";
		h.writeFileToHDFS("hdfs://localhost:9000/user/hadoop/CommunitySearch/"+file+"/"+file+"CommunitySearch/TimeIndex",content);
		System.exit(0);
	}
	

}
