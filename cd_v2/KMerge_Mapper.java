package cd_v2;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class KMerge_Mapper extends Mapper<LongWritable, Text, Text, Text>{

	/**
	 * map函数
	 * 输入:key:文件内容的偏移量
	 *     value:文件的一行内容(图中的极大完全子图(派系))
	 * 输出:key:某个派系中的最小值的点
	 *     value:那个点所在的派系
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		List<Integer> clique = new LinkedList<Integer>();// 其中的一个派系
		for(String node : line.substring(1,line.length()-1).split(",")){
			clique.add(Integer.valueOf(node.trim()));
		}
		
		int min = clique.get(0);
		for(int i = 1;i<clique.size();i++) {//找出派系中的最小值的点
			if(clique.get(i) < min) {
				min = clique.get(i);
			}
		}
		Text newKey = new Text();
		newKey.set(Integer.toString(min));
		context.write(newKey,value);
	}
}

