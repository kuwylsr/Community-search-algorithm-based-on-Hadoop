package cs_v1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CS_Reducer extends Reducer<Text, Text, Text, Text>{
	
	/**
	 * 进行社区查找的reduce函数
	 * 输入:key:用户的请求标签
	 *     value:包含用户请求节点集合的倒排索引的迭代器
	 * 输出:key:用户的请求标签
	 *     value:满足用户查询请求的社区ID
	 */
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		String[] temp = null;
		Set<String> strSet1 = new HashSet<>();
		if(iter.hasNext()) {
			temp = iter.next().toString().split("\t")[1].split(",");	
			CollectionUtils.addAll(strSet1, temp);
		}
		while(iter.hasNext()) {//遍历所有包含用户请求节点的倒排索引,找出所有集合的交集
			String[] content = iter.next().toString().split("\t")[1].split(",");
			Set<String> strSet2 = new HashSet<>();
			CollectionUtils.addAll(strSet2, content);
			strSet1.retainAll(strSet2);//计算集合的交集
		}
		Text t = new Text();
		t.set(strSet1.toString());
		context.write(key, t);
	}
}
