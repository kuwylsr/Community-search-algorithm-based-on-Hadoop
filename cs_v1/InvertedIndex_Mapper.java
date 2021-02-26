package cs_v1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndex_Mapper extends Mapper<LongWritable, Text, Text, Text>{
	
	/**
	 * 建立倒排索引的map函数
	 * 输入:key:文件内容的偏移量(文件为聚类后的所有社区文件)
	 *     value:文件一行的内容
	 * 输出:key:社区中的一个节点node
	 *     value:对应文件的一行内容(社区ID-社区内容)
	 */
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String community = line.split("\t")[1];
		for(String node : community.substring(1, community.length()-1).split(",")) {
			Text newKey = new Text();
			newKey.set(node.trim());
			context.write(newKey, value);
		}
	}
}
