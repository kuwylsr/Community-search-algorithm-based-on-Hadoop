package cs_v1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndex_Reducer extends Reducer<Text, Text, Text, Text>{
	
	/**
	 * 建立倒排索引的reduce函数
	 * 输入:key:社区中的某个节点node
	 *     value:包含该节点node的所有社区的迭代器
	 * 输出:key:输入key的节点node
	 *     value:包含该节点的所有社区的内容(目标社区的IDs 目标社区的内容s)
	 */
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		String communityID = "";
		String community = "";
		while(iter.hasNext()) {
			String temp = iter.next().toString();
			communityID = communityID + temp.split("\t")[0] + ",";
			community = community + temp.split("\t")[1] + ",";
		}
		Text t = new Text();
		t.set(communityID + "\t" + community);
		context.write(key, t);
	}
}
