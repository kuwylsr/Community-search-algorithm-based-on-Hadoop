package cs_v1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CS_Mapper extends Mapper<LongWritable, Text, Text, Text>{

	//查询社区的输入值
	//其中不同用户之间的查询用'\n'隔开
	//一个用户所要查找的不用属性信息用';'隔开
	private static String userInput = "165682,164172;440452,380679"+"\n"+
										"2068250,2068244,1223327;1199394,781595"+"\n"+
										"627214,156171,152072,313876;30887,30940";
	
	/**
	 * 判断当前节点是否在用户请求的目标节点集合当中
	 * @param CurNode
	 * @param targets
	 * @return
	 */
	public boolean ContainTarget(String CurNode,String targets) {
		String[] nodes = targets.split(",");
		for(int i=0;i<nodes.length;i++) {
			if(CurNode.equals(nodes[i])) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 进行社区查找的map函数
	 * 输入:key:文件内容的偏移量(文件为社区节点的倒排索引文件)
	 *     value:文件一行的内容(社区中的某个节点node: 包含该节点的所有社区IDs 各个社区的内容)
	 * 输出:key:用户请求的标签
	 *     value:文件一行的内容(输入的value值)
	 */
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String node = line.split("\t")[0];
		String[] UInput = userInput.split("\n");
		for(int i=0;i<UInput.length;i++) { //生成不同用户的请求信息
			String[] Input = UInput[i].split(";");
			Text request = new Text();
			String temp = "The "+i+"th User || ";
			for(int j=0;j<Input.length;j++) { //生成同一用户的不同请求信息
				if(ContainTarget(node,Input[j])) {//如果当前节点在用户所要请求的信息当中
					temp = temp + "The "+j+"th request:";
					request.set(temp);
					context.write(request, value);
				}
			}
		}
		
	}
}
