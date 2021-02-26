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

public class Clique_Reducer extends Reducer<Text, Text, Text, Text> {

	//定义一个存放根节点的列表(根节点即为map函数输出的key值)
	private List<Integer> route = new LinkedList<Integer>(); 
	//一个根节点的map
	//其中map的key值为根节点的某个邻居(也就是map函数输入的key值)
	//map的value值为 map中key值的邻居集合(也就是根节点的邻居的邻居集合)
	private Map<Integer, Set<Integer>> dict = new HashMap<Integer, Set<Integer>>();
	
	/**
	 * 选出在根节点root的邻居中,和其他root邻居节点邻接最多的节点
	 * @param neighbors root的邻居节点
	 * @return neighbors中和neighbors中其他节点邻接最多的顶点
	 */
	public Integer maxNeighbor(Set<Integer> neighbors) {
		if (neighbors.size() == 0)
			return null;
		int max = -1; //定义邻接数量最多的满足要求的目标节点
		int maxConnectNum = -1; //定义最大的邻接数量
		for (int neighbor : neighbors) {
			int num = 0; //邻接的数量
			for (int secondNeighbor : dict.get(neighbor))// 遍历root邻居的邻居节点集合
				if (neighbors.contains(secondNeighbor))
					num += 1;
			if (num > maxConnectNum) {
				maxConnectNum = num;
				max = neighbor;
			}
		}
		return max;
	}

	/**
	 * 求两个集合的交集
	 * @param setA
	 * @param setB
	 * @return
	 */
	public Set<Integer> join(Set<Integer> setA, Set<Integer> setB) {
		Set<Integer> newSet = new HashSet<Integer>();
		for (int node : setA) {
			if (setB.contains(node)) {
				newSet.add(node);
			}	
		}
		return newSet;
	}

	/**
	 * 求两个集合的差集 A-B = A-AB
	 * @param setA
	 * @param setB
	 * @return
	 */
	public Set<Integer> minus(Set<Integer> setA, Set<Integer> setB) {
		Set<Integer> newSet = new HashSet<Integer>();
		for (int node : setA) {
			if (!setB.contains(node)) {
				newSet.add(node);
			}
		}
		return newSet;
	}

	/**
	 * 深度复制集合Set
	 * @param s
	 * @return
	 */
	public Set<Integer> deepCopySet(Set<Integer> s) {
		Set<Integer> newSet = new HashSet<Integer>();
		for (int n : s)
			newSet.add(n);
		return newSet;
	}
	
	/**
	 * 深度复制列表
	 * @param s
	 * @return
	 */
	public List<Integer> deepCopyList(List<Integer> s) {
		List<Integer> newList = new LinkedList<Integer>();
		for (int n : s)
			newList.add(n);
		return newList;
	}

	/**
	 * 打印派系信息
	 * @return
	 */
	public String printClique() {
		boolean tag = true;
		for(int i=1;i<route.size();i++) {
			if(route.get(0) > route.get(i)) {
				tag = false;  
			}
		}	 
		String res = "";
		if(tag) {
			res = route.toString();
		}
		return res;
	}

	/**
	 * 通过回溯法递归地查找最大完全子图
	 * @param root 根节点
	 * @param neighbors 根节点的邻居节点
	 * @param hasVisited 用来记录节点是否被访问过的集合
	 * @return
	 */
	public List<String> buildTree(int root, Set<Integer> neighbors,Set<Integer> hasVisited) {
		if (neighbors == null) {
			return new LinkedList<String>();
		}
		List<String> res = new LinkedList<String>(); //定义查找到的所有派系的集合
		if (neighbors.size() == 0) { //递归终止条件,根节点的邻居节点个数为0
			String clique = printClique();
			if(!"".equals(clique))
				res.add(clique);
		} else {
			int next = maxNeighbor(neighbors); // 找出与根节点的邻居节点链接最多的邻居节点
			Set<Integer> cand = minus(neighbors, dict.get(next));// 求出与root邻接但不与next邻接的节点集合
			while (true) {
				cand.remove(next); //移除next本身
				route.add(next);// 将next加入根节点当中
				if(!hasVisited.contains(next)) { //如过next节点没有被访问过
					hasVisited.add(next); //将next节点设置为已经访问过
					//将next作为根节点,传入与原root和next都邻接的节点的集合
					res.addAll(buildTree(next, join(neighbors, dict.get(next)), deepCopySet(hasVisited))); //递归地查找所有派系
				}
				if (cand.size() == 0) { //如果不存在与root邻接但不与next邻接的节点(找到了一个派系)
					route.remove(route.size() - 1);//根节点集合中去掉最后一个next节点
					break;//结束某个根节点的深度搜索
				}
				next = maxNeighbor(cand);// 继续进行深度搜索
				route.remove(route.size() - 1);//去除掉已经搜索到尽头的所有一个顶点
			}
		}
		return res;
	}
	/**
	 * reduce函数
	 * 输入:key:node邻居中的某个节点
	 *     value:node:neighbors"的形式进行输出,其中neighbors中不包含key值
	 * 输出:key:null
	 *     value:图中的最有最大完全子图(子图中的顶点个数大于等于3)
	 */
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		int root = Integer.valueOf(key.toString());
		route.add(root);//将key值作为根节点加入根节点集合
		Iterator<Text> iter = values.iterator();
		Set<Integer> directNeighbors = new HashSet<Integer>(); //与根节点root直接相邻的节点的集合
		
		while (iter.hasNext()) {
			String line = iter.next().toString();
			int index = line.indexOf(":");
			String neighbor = line.substring(0, index); //根节点的邻居节点
			directNeighbors.add(Integer.valueOf(neighbor));
			if (index == line.length() - 1) { //如果根节点的邻居节点只有一个邻居,就是root(只有root一个邻居)
				dict.put(Integer.valueOf(neighbor), new HashSet<Integer>());
				continue;
			}
			String[] secondNeighbors = line.substring(index + 1).split(","); //root邻居节点的邻居节点
			Set<Integer> s = new HashSet<Integer>();//root邻居节点的邻居节点集合
			for (int i = 0; i < secondNeighbors.length; i++)
				s.add(Integer.valueOf(secondNeighbors[i]));
			dict.put(Integer.valueOf(neighbor), s);
		}
		List<String> cliques = buildTree(root, directNeighbors, new HashSet<Integer>()); //查找图中的所有最大完全子图
		for(String clique : cliques){ //过滤掉完全子图中顶点个数少于3个的子图
			if(clique.split(",").length < 3) {
				continue;
			}
			Text t = new Text();
			t.set(clique);
			context.write(null,t);
		}
		route.remove(route.size()-1);//去掉root节点
	}
}

