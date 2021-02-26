package cd_v2;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KAllCombined_Reducer extends Reducer<Text, Text, Text, Text> {

	/**
	 * 判断两个派系能否合并(是否邻接)
	 * @param c1
	 * @param c2
	 * @return
	 */
	public boolean isAdjacent(Set<Integer> c1,Set<Integer> c2) {
		int common = 0;
		for(int n1 : c1) {
			if(c2.contains(n1)) {
				common++;
			}
		}		
		int min = c1.size(); //将k值设为两个派系集合大小的最小值
		if(c2.size() < min) {
			min = c2.size();
		}
		if(common >= 3) {//如果两个派系的公共邻居数大于等于k-1,就可以进行合并
			return true;
		}else {
			return false;
		}	
	}

	/**
	 * 已经合并的派系是否包含与新派系临接的派系
	 * @param cliques
	 * @param clique
	 * @return -1表示不包含,其他值表示与指定团临接的团的index
	 */
	public int contain(List<Set<Integer>> cliques, Set<Integer> clique) {
		for(int i=0;i<cliques.size();i++) {
			if(isAdjacent(cliques.get(i), clique))
				return i;
		}
		return -1;
	}
	/**
	 * reduce函数
	 * 输入:key:派系中的某个顶点
	 *     value:包含这个顶点的所有派系(CliqueID+"\t"+派系的内容)的格式
	 * 输出:key:null
	 *     value:能合并的两个派系的ID
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<Set<Integer>> cliques = new LinkedList<Set<Integer>>(); //包含key值的所有派系的集合
		Map<Integer, Integer> index_cliqueID = new HashMap<Integer, Integer>();//定义派系集合的下表到派系ID的映射
		Iterator<Text> iter = values.iterator();
		while (iter.hasNext()) {
			String[] contents = iter.next().toString().split("\t");
			int cliqueID = Integer.valueOf(contents[0]); //派系的ID
			Set<Integer> clique = new HashSet<Integer>(); //派系
			for (String node : contents[1].substring(1, contents[1].length() - 1).split(",")) {
				clique.add(Integer.valueOf(node.trim()));
			}
			cliques.add(clique);
			index_cliqueID.put(cliques.size() - 1, cliqueID);//保存相应的映射
		}
		
		if (cliques.size() == 1) {//如果只有一个派系,结束合并
			return;
		}
		//遍历包含输入key值的所有派系的集合
		for (int i = 0; i < cliques.size(); i++) {
			for (int j = i + 1; j < cliques.size(); j++) {
				if (isAdjacent(cliques.get(i), cliques.get(j))) {//若两个派系能进行合并
					int clique_i = index_cliqueID.get(i);
					int clique_j = index_cliqueID.get(j);
					Text t = new Text();
					//将能合并的两个派系的ID由小到大进行排列
					if(clique_i < clique_j) {
						t.set(clique_i+","+clique_j);
					}else {
						t.set(clique_j+","+clique_i);
					}		
					context.write(null, t);
				}
			}
		}			
	}
}

