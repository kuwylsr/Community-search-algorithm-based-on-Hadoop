package cd_v1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Scan_Mapper extends Mapper<LongWritable, Text, Text, Text> {
	/**
	 * 转换邻居写出的格式
	 * @param neighbors 邻居的数组
	 * @return 转换后的格式
	 */
	public String convert(String[] neighbors) {
		StringBuilder sb = new StringBuilder();
		for (String s : neighbors) {
			sb.append(s);
			sb.append(",");
		}
		return sb.substring(0, sb.length() - 1).toString();
	}
	
	/**
	 * map类
	 * 输入:文件中的行信息,其中每一行为 node:neighbors
	 *     key为偏移量
	 *     value为每一行的内容
	 * 输出:key:一条边信息;
	 *     value:这条边中某个节点的邻居信息;
	 * 
	 * 原理:这样当进行shuffle的过程中,同一条边将会被归并到一起;而对应的value集合则为这两个顶点的邻居集合(这样也就方便求出共同邻居的数目)
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		int index = line.indexOf(":");
		if (index == -1)
			return;
		String node = line.substring(0, index); //取出行信息中的节点信息
		String[] neighbors = line.substring(index + 1).split(","); //取出行信息中的邻居信息
		for (String neighbor : neighbors) {
			Text edge = new Text();
			// 将邻居信息按照节点序号有大到小排列输出
			if (Integer.valueOf(node) < Integer.valueOf(neighbor)) {
				edge.set(node + "," + neighbor);
			} else {
				edge.set(neighbor + "," + node);
			}
			Text t = new Text();
			t.set(convert(neighbors));
			context.write(edge, t);// 输出一条边,和边中一个节点的邻居集合
		}

	}
}
