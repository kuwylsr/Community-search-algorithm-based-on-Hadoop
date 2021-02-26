package cd_v2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HandleData {

	/**
	 * 从HDFS分布式文件系统中读取文件内容
	 * @param filePath HDFS中的目标文件路径
	 * @return 返回文件内容的字符串格式
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static String readFileFromHDFS(String filePath) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		Path srcPath = new Path(filePath);
		
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		
		String content = "";
		byte[] ioBuffer = new byte[1024]; // 每次从文件系统中读取一个字节的数据
		int readLen = hdfsInStream.read(ioBuffer);
		while(readLen!=-1)
		{
			content = content + new String(ioBuffer, 0, readLen);
			readLen = hdfsInStream.read(ioBuffer);
		}
		hdfsInStream.close();
		fs.close();
		return content;
	}
	
	/**
	 * 将数据写入HDFS当中
	 * @param filePath 输出文件的路径
	 * @throws IOException 
	 */
	public void writeFileToHDFS(String filePath,String content) throws IOException {
		Configuration conf = new Configuration();
		Path dstPath = new Path(filePath);
		FileSystem fs = dstPath.getFileSystem(conf);
		
		FSDataOutputStream outputStream = fs.create(dstPath);
		outputStream.write(content.getBytes());
		outputStream.close();
		fs.close();
	}
	
	/**
	 * 处理合并派系的标准,将所有能合并的派系的ID放到一个集合当中
	 * @param MergedStandard
	 * @return 处理后的标准(其中字符串数组中的每一个元素为一个能合并的所有派系)
	 */
	public Set<Set<Integer>> HandleStandard(String MergedStandard) {
		//定义一个倒排索引,其中key值为一个可以进行合并和派系的ID,value值为所有可以和这个ID派系进行合并的派系的ID的集合
		Map<Integer, Set<Integer>> map = new HashMap<>();
		int length = MergedStandard.trim().split("\n").length;
		int num = 1;
		for(String line : MergedStandard.trim().split("\n")) {//遍历标准中的每一对可以进行合并的派系
//			num ++;
//			System.out.println(num +"-->"+length);
			int ID1 = Integer.parseInt(line.split(",")[0]);
			int ID2 = Integer.parseInt(line.split(",")[1]);

//			//如果map中包含第一个派系的ID(第一个派系之前已经被合并过),且包含第二个派系的ID(第二个派系之前也被合并过)
//			if(map.containsKey(ID1)&&map.containsKey(ID2)) {
//				Set<Integer> temp = new HashSet<>();
//				map.get(ID1).addAll(map.get(ID2)); //将可以与ID1进行合并的集合和能与ID2进行合并的集合求并集
//				temp.addAll(map.get(ID1)); //定义一个新的集合,并将其赋值成上并集
//				for(int id : temp) {
//					map.put(id, temp); //更新所有的并集中的ID索引,让他们的value值为同一个索引集合
//				}
//			//如果map中只包含第一个派系的ID(只有第一个派系被合并过)
//			}else if(map.containsKey(ID1)){
//				map.get(ID1).add(ID2);//将可以与ID1进行合并的集合中加入ID2
//				Set<Integer> temp = map.get(ID1);
//				map.put(ID2,temp);//建立ID2的倒排索引,注意能与ID2进行合并的派系的集合与ID1是同一个
//			//如果map中只包含第二个派系的ID(只有第二个派系被合并过),同上
//			}else if(map.containsKey(ID2)) {
//				map.get(ID2).add(ID1);
//				Set<Integer> temp = map.get(ID2);
//				map.put(ID1,temp);
//			//如果两个派系之前都没有被合并过
//			}else {
//				Set<Integer> s = new HashSet<>();
//				s.add(ID1);
//				s.add(ID2);
//				map.put(ID1, s);//建立ID1的倒排索引
//				map.put(ID2, s);//建立ID2的倒排索引(与ID1能进行合并的派系的集合使用一个)
//			}
			int flag1 = 0;
			int flag2 = 0;
			if(map.containsKey(ID1)) {
				flag1 = 1;
			}
			if(map.containsKey(ID2)) {
				flag2 = 1;
			}
			//如果map中包含第一个派系的ID(第一个派系之前已经被合并过),且包含第二个派系的ID(第二个派系之前也被合并过)
			if((flag1 == 1) && (flag2 == 1)) {
				Set<Integer> temp = new HashSet<>();
				map.get(ID1).addAll(map.get(ID2)); //将可以与ID1进行合并的集合和能与ID2进行合并的集合求并集
				temp.addAll(map.get(ID1)); //定义一个新的集合,并将其赋值成上并集
				for(int id : temp) {
					map.put(id, temp); //更新所有的并集中的ID索引,让他们的value值为同一个索引集合
				}
			//如果map中只包含第一个派系的ID(只有第一个派系被合并过)
			}else if(flag1 == 1) {
				map.get(ID1).add(ID2);//将可以与ID1进行合并的集合中加入ID2
				Set<Integer> temp = map.get(ID1);
				map.put(ID2,temp);//建立ID2的倒排索引,注意能与ID2进行合并的派系的集合与ID1是同一个
			//如果map中只包含第二个派系的ID(只有第二个派系被合并过),同上
			}else if(flag2 == 1) {
				map.get(ID2).add(ID1);
				Set<Integer> temp = map.get(ID2);
				map.put(ID1,temp);
				//如果两个派系之前都没有被合并过
			}else {
				Set<Integer> s = new HashSet<>();
				s.add(ID1);
				s.add(ID2);
				map.put(ID1, s);//建立ID1的倒排索引
				map.put(ID2, s);//建立ID2的倒排索引(与ID1能进行合并的派系的集合使用一个)
			}
		}
		Set<Set<Integer>> s = new HashSet<>();//利用set集合的互异性,进行筛选
		for(Map.Entry<Integer, Set<Integer>> entry: map.entrySet()) {
			s.add(entry.getValue());
		}
		return s;
	}
	
	/**
	 * 合并派系
	 * @param MergedStandardH
	 * @param IDToContent
	 * @return
	 */
	public Map<Integer,Set<Integer>> MergedClique(Set<Set<Integer>> MergedStandardH , Map<Integer,Set<Integer>> IDToContent){
		for(Set<Integer> newStandard : MergedStandardH) {//将每一行标准中的派系都进行合并
			List<Integer> templist = new ArrayList<Integer>(newStandard);
			int theFirstID = templist.get(0);
			for(int id : newStandard) {
				IDToContent.get(theFirstID).addAll(IDToContent.get(id));
				if(id != theFirstID) {
					IDToContent.remove(id);
				}
			}
		}
		return IDToContent;
	}
	public void Handle(String file) throws IOException, URISyntaxException {
		//从HDFS中读取待所有派系的信息
		String iniMergedContent = readFileFromHDFS("hdfs://localhost:9000/user/hadoop/cpmOutput/cpm"+file+"Output/iniMerged/part-r-00000");
		//从HDFS中读取合并派系标准
		String MergedStandard = readFileFromHDFS("hdfs://localhost:9000/user/hadoop/cpmOutput/cpm"+file+"Output/ck-graph/part-r-00000"); 
		
		Map<Integer,Set<Integer>> IDToContent = new HashMap<>();//建立一个派系ID到派系内容的映射
		for(String line : iniMergedContent.trim().split("\n")) {
			int cliqueID = Integer.parseInt(line.split("\t")[0]);
			String cliqueS = line.split("\t")[1];
			
			Set<Integer> clique = new HashSet<>();
			for(String node : cliqueS.substring(1,cliqueS.length()-1).split(",")){
				clique.add(Integer.valueOf(node.trim()));
			}
			IDToContent.put(cliqueID, clique);
		}
		
		Set<Set<Integer>> MergedStandardH = HandleStandard(MergedStandard);//将合并派系的标准进行处理
		Map<Integer,Set<Integer>> cliques = MergedClique(MergedStandardH, IDToContent);//合并所有能进行合并派系
		String content = "";
		int num = -1;
		for(Map.Entry<Integer, Set<Integer>> entry : cliques.entrySet()) {
			num++;
			content = content + Integer.toString(num) + "\t" + entry.getValue()+"\n";
		}
		writeFileToHDFS("hdfs://localhost:9000/user/hadoop/cpmOutput/cpm"+file+"Output/finalResult/FinalClique",content);
	}
}
