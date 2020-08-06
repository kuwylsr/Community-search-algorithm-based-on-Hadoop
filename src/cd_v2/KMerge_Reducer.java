package cd_v2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMerge_Reducer extends Reducer<Text, Text, Text, Text> {
	
	private static int cliqueID = 1;
	
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
		if(common >= min-1) {//如果两个派系的公共邻居数大于等于k-1,就可以进行合并
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
	 * 输入:key:某几个派系中的公共最小点
	 *     value:包含key的所有派系
	 * 输出:key:合并后的派系的ID
	 *     value:合并后的派系
	 */
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<Set<Integer>> cliques = new LinkedList<Set<Integer>>();//包含某个点的所有派系的集合
		Iterator<Text> iter = values.iterator();
		while(iter.hasNext()){
			String line = iter.next().toString();
			Set<Integer> clique = new HashSet<Integer>();//某个派系
			for(String node : line.substring(1,line.length()-1).split(",")){
				clique.add(Integer.valueOf(node.trim()));
			}
			cliques.add(clique);
		}
		
		List<Set<Integer>> mergedClique = new LinkedList<Set<Integer>>();//合并后的所有派系的集合
		for(Set<Integer> clique : cliques) {
			int index = contain(mergedClique,clique);//判断已经合并的派系是否包含与新派系临接的派系(是否有派系还可以与已经合并的派系继续进行合并)
			if(index == -1) {
				mergedClique.add(clique);
			}else {
				mergedClique.get(index).addAll(clique);
			}
		}
		
		for(Set<Integer> clique : mergedClique){
			Text newKey = new Text();
			newKey.set(Integer.toString(cliqueID++));
			Text t = new Text();
			t.set(clique.toString());
			context.write(newKey, t);
		}
	}
}
