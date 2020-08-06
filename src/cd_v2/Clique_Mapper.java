package cd_v2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Clique_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	/**
	 * 转换邻居的格式,并且去除掉一个目标邻居(该邻居将作为key值进行输出)
	 * 
	 * @param contents 要进行格式转换的邻居
	 * @param index    目标邻居节点(不进行格式转换)
	 * @return 转化之后的邻居列表
	 */
	public String convert(String[] contents, int index) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < contents.length; i++) {
			if (i != index) { // 对目标节点不进行转换
				sb.append(contents[i]);
				sb.append(",");
			}
		}
		if (sb.length() != 0)
			sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}

	/**
	 * map函数 
	 * 输入:key:文件内容的偏移量 
	 *     value:文件中每一行的内容(node:neighbors的形式)
	 *      
	 * 输出:key:node的邻居中的某个节点
	 *     value:以"node:neighbors"的形式进行输出,其中neighbors中不包含key值
	 * 
	 * 经过map的shuffle过程之后,就得到了key的邻居以及它邻居的邻居
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		int index = line.indexOf(":");
		if (index == -1)
			return;
		String node = line.substring(0, index); // 获取顶点
		String[] neighbors = line.substring(index + 1).split(","); //获取顶点的邻居列表
		for (int i = 0; i < neighbors.length; i++) {
			Text newKey = new Text();
			newKey.set(neighbors[i]);//将node的某个邻居节点作为输出的key值
			Text newLine = new Text();
			newLine.set(node + ":" + convert(neighbors, i));//输出的value值的格式
			context.write(newKey, newLine);
		}

	}

}
