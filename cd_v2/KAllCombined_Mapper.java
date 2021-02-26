package cd_v2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class KAllCombined_Mapper extends Mapper<LongWritable, Text, Text, Text>{

	/**
	 * map函数
	 * 输入:key:文件内容的偏移量
	 *     value:文件中的一行内容(CliqueID+"\t"+派系的内容)
	 * 输出:key:派系中的某个顶点
	 *     value:CliqueID+"\t"+派系的内容
	 * 
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String clique = line.split("\t")[1];
		for(String node : clique.substring(1,clique.length()-1).split(",")){
			Text newKey = new Text();
			newKey.set(node);
			context.write(newKey,value);
		}
	}
}

