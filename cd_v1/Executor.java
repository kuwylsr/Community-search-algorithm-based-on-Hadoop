package cd_v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Executor {

	private static String file = "Dolphin";
	public static void main(String[] args) throws Exception {
//		 Configuration conf = new Configuration();
//		if (args.length != 2) {
//			System.err.println("Usage: wordcount <in> <out>");
//			System.exit(2);
//		}

		for (int iterTime = 1; iterTime < 10; iterTime++) {
//			if(iterTime == 9) {
//				continue;
//			}
//			Job job = Job.getInstance(conf);
			Job job = new Job();
			job.setJarByClass(Executor.class);
			job.setJobName("scan");

			double alpha = iterTime / 10.0;// 每次循环给定不同的阈值

			// HDFS上的输入文件和输出文件的位置
			Path inputPath = new Path("scanInput/scan"+file+".txt");//绝对路径
			Path outputPath = new Path("scanOutput/scan"+file+"Output/pscan_" + alpha);//绝对路径
			
					
			FileInputFormat.addInputPath(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputPath);

			Configuration conf = job.getConfiguration();
			System.out.println("alpha = " + alpha);
			conf.set("alpha", Double.toString(alpha));
			
			// 判断输出路径存不存在，若存在就删除。
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(outputPath)) {
				fs.delete(outputPath,true);
			}	
			job.setMapperClass(Scan_Mapper.class);
			job.setReducerClass(Scan_Reducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.waitForCompletion(true);

			new HandleData().Handle(file,Double.toString(alpha)); //将结果的形式变为ID-社区内容的格式
			new FScoreIndex().CalculateFScore(file,Double.toString(alpha));//计算聚类指标F-Score
		}
		System.exit(0);
	}
}
