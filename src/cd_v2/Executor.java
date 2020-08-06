package cd_v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Executor {
	
	private static String file = "TestExperimentData100";

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();//获取当前时间
		long startTime1 = System.currentTimeMillis();//获取当前时间
		Configuration conf = new Configuration();
//		if (args.length != 2) {
//			System.err.println("Usage: wordcount <in> <out>");
//			System.exit(2);
//		}

			
		// 该段代码是用来判断输出路径存不存在，若存在就删除。
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path("cpmOutput/cpm"+file+"Output/"))) {
			fs.delete(new Path("cpmOutput/cpm"+file+"Output/"),true);
		}	
			
		// 第一个mapreduce来求出图中的所有极大完全子图(派系)
		Job cliqueGenerator = new Job();
		cliqueGenerator.setJarByClass(Executor.class);
		cliqueGenerator.setJobName("clique generator");
		
		FileInputFormat.addInputPath(cliqueGenerator, new Path("cpmInput/cpm"+file+".txt"));
		FileOutputFormat.setOutputPath(cliqueGenerator, new Path("cpmOutput/cpm"+file+"Output/allCliques"));

		cliqueGenerator.setMapperClass(Clique_Mapper.class);
		cliqueGenerator.setReducerClass(Clique_Reducer.class);
		// job.setNumReduceTasks(4);
		cliqueGenerator.setOutputKeyClass(Text.class);
		cliqueGenerator.setOutputValueClass(Text.class);

		cliqueGenerator.waitForCompletion(true);

		// 第二个mapreduce来进行k-派系的合并
		Job iniMerger = new Job();
		iniMerger.setJarByClass(Executor.class);
		iniMerger.setJobName("initial merger");

		FileInputFormat.addInputPath(iniMerger, new Path("cpmOutput/cpm"+file+"Output/allCliques"));
		FileOutputFormat.setOutputPath(iniMerger, new Path("cpmOutput/cpm"+file+"Output/iniMerged"));

		iniMerger.setMapperClass(KMerge_Mapper.class);
		iniMerger.setReducerClass(KMerge_Reducer.class);
		// job.setNumReduceTasks(4);
		iniMerger.setOutputKeyClass(Text.class);
		iniMerger.setOutputValueClass(Text.class);

		iniMerger.waitForCompletion(true);

		// 第三个mapreduce来进行对合并后的k-派系进行合并扫描
		Job graphConstructor = new Job();
		graphConstructor.setJarByClass(Executor.class);
		graphConstructor.setJobName("ck-graph constructor");

		FileInputFormat.addInputPath(graphConstructor, new Path("cpmOutput/cpm"+file+"Output/iniMerged"));
		FileOutputFormat.setOutputPath(graphConstructor, new Path("cpmOutput/cpm"+file+"Output/ck-graph"));

		graphConstructor.setMapperClass(KAllCombined_Mapper.class);
		graphConstructor.setReducerClass(KAllCombined_Reducer.class);
		// job.setNumReduceTasks(4);
		graphConstructor.setOutputKeyClass(Text.class);
		graphConstructor.setOutputValueClass(Text.class);

		graphConstructor.waitForCompletion(true);
		long endTime1 = System.currentTimeMillis();
		System.out.println("社区划分算法运行时间:"+(double)(endTime1-startTime1)/1000 + "秒");
		
		long startTime2 = System.currentTimeMillis();//获取当前时间
		HandleData h = new HandleData();
		h.Handle(file); //将最终的结果转化为ID-社区内容的格式
		long endTime2 = System.currentTimeMillis();
		System.out.println("社区格式转化运行时间:"+(double)(endTime2-startTime2)/1000 + "秒");
		
		long startTime3 = System.currentTimeMillis();//获取当前时间
		new FScoreIndex().CalculateFScore(file);//计算评价聚类的指标F-Score
		long endTime3 = System.currentTimeMillis();
		System.out.println("计算聚类评价指标运行时间:"+(double)(endTime3-startTime3)/1000 + "秒");
		
		long endTime = System.currentTimeMillis();
		System.out.println("程序运行总时间:"+(double)(endTime-startTime)/1000 + "秒");
		
		String content = "查询的文件为: "+file+"\n"+
				"社区划分算法运行时间:"+(double)(endTime1-startTime1)/1000 + "秒" + "\n" +
				"社区格式转化运行时间:"+(double)(endTime2-startTime2)/1000 + "秒"+"\n"+
				"计算聚类评价指标运行时间:"+(double)(endTime3-startTime3)/1000 + "秒"+"\n"+
				"程序运行总时间:"+(double)(endTime-startTime)/1000 + "秒";
		h.writeFileToHDFS("hdfs://localhost:9000/user/hadoop/cpmOutput/cpm"+file+"Output/finalResult/TimeIndex",content);
		System.exit(0);
	}

}
