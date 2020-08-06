package cd_v1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Scan_Reducer extends Reducer<Text, Text, Text, Text> { 
	/**
	 * 计算结点的结构化相似度
	 * @param s1 顶点1的邻居信息
	 * @param s2 顶点2的邻居信息
	 * @return 两个顶点之间的相似度
	 */
	public double calSim(Set<Integer> s1, Set<Integer> s2) {
		double common = 2.0; //共同邻居数的初始值为2,因为要计算上边的两个顶点
		for (Integer i : s1) {
			if (s2.contains(i)) {
				common += 1.0;
			}
		}
		return common / Math.sqrt((s1.size() + 1) * (s2.size() + 1));// 根据Salton指标来计算相似度指标
	}
	
	/**
	 * reduce类
	 * 输入:经过shuffle的过程,输入的键值对为
	 *     key:一条边信息
	 *     value:此条边当中各个点所包含的邻居信息
	 * 输出:此条边的两个顶点的相似度大于阈值的键值对
	 *     key:null
	 *     value:边信息
	 * 达到了只要两个顶点之间有边,就计算他们的相似度的目的
	 * 
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double alpha = Double.valueOf(context.getConfiguration().get("alpha")); //获取设定参数中的阈值
		Iterator<Text> iter = values.iterator();
		Set<Integer> set1 = new HashSet<Integer>();
		Set<Integer> set2 = new HashSet<Integer>();
		for (String s : iter.next().toString().split(",")) {//set1中添加第一个节点的邻居信息
			set1.add(Integer.valueOf(s));
		}
		try {
			for (String s : iter.next().toString().split(",")) {//set2中添加第二个节点的邻居信息
				set2.add(Integer.valueOf(s));
			}
		} catch (Exception e) {
			System.out.println("Error: " + key);
		}
//		for (String s : iter.next().toString().split(",")) {//set2中添加第二个节点的邻居信息
//			set2.add(Integer.valueOf(s));
//		}
		double sim = calSim(set1, set2);//计算两个节点的相似度
		if (sim >= alpha) {
			context.write(null, key);
		}
	}
}