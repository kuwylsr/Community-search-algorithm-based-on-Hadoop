package cd_v1;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FScoreIndex {

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
	public static void writeFileToHDFS(String filePath,String content) throws IOException {
		Configuration conf = new Configuration();
		Path dstPath = new Path(filePath);
		FileSystem fs = dstPath.getFileSystem(conf);
		
		FSDataOutputStream outputStream = fs.create(dstPath);
		outputStream.write(content.getBytes());
		outputStream.close();
		fs.close();
	}
	
	/**
	 * 寻找与所划分社区相似度最高的标准社区
	 * @param resMap
	 * @param sta
	 * @return
	 */
	public static String MaxSameCommunity(Map<String,Set<String>> resMap,Set<String> sta) {
		String maxID = "1";
		int maxNum = 0;
		for(Entry<String, Set<String>> entry : resMap.entrySet()) { //遍历所有的聚类结果集合
			Set<String> copy = new HashSet<>();
			copy.addAll(entry.getValue());
			copy.retainAll(sta);
			int num = copy.size();
			if(num > maxNum) {
				maxNum = num;
				maxID = entry.getKey();
			}
		}
		return maxID;
	}
	
	/**
	 * 计算以sta为正例社区的召回率(将正类预测为正类 / 所有正真的正类)
	 * @param res 被预测为正例的集合
	 * @param sta 实际为正例的集合
	 * @return
	 */
	public static double calRecall(Set<String> res,Set<String> sta) {
		int Nplus = sta.size();
		Set<String> copy = new HashSet<>();
		copy.addAll(res);
		copy.retainAll(sta);//求交集
		int TP = copy.size();
		return (double)TP/Nplus;
	}
	
	/**
	 * 计算以sta为正例社区的精确率(将正类预测为正类 / 所有预测为正类)
	 * @param res 被预测为正例的集合
	 * @param sta 实际为正例的集合
	 * @return
	 */
	public static double calPrecision(Set<String> res,Set<String> sta) {
		Set<String> copy = new HashSet<>();
		copy.addAll(sta);
		copy.retainAll(res);//求交集
		int TP = copy.size();
		int Pplus = res.size();
		return (double)TP/Pplus;
	}
	
	/**
	 * 计算F-Score聚类指标
	 * @param recallMap 以每个标准社区为正例的召回率映射
	 * @param precision 以每个标准社区为正例的精确率集合
	 * @return
	 */
	public static double calFScore(Map<String,Double> recallMap,Map<String,Double> precision) {
		double avgRecall = 0;
		double avgPrecision = 0;
		double Bate = 1;
		for(Entry<String, Double> entry : recallMap.entrySet()) {
			avgRecall = avgRecall + entry.getValue();
		}
		for(Entry<String, Double> entry : precision.entrySet()) {
			avgPrecision = avgPrecision + entry.getValue();
		}
		avgRecall = avgRecall / recallMap.size();
		avgPrecision = avgPrecision / precision.size();
		double FScore = (avgRecall * avgPrecision) / (avgRecall + Math.pow(Bate, 2)*avgPrecision);
		return (1 + Math.pow(Bate, 2))*FScore;
	}
	
	public void CalculateFScore(String file,String alpha) throws IOException, URISyntaxException {
		//读取标准的社区集合
		String StandardCommunity = readFileFromHDFS("hdfs://localhost:9000/user/hadoop/StandardFile/StandardCommunity"+file+".txt"); 
		//读取聚类结果的社区集合
		String ResultCommunity;
		try {
			ResultCommunity = readFileFromHDFS("hdfs://localhost:9000/user/hadoop/scanOutput/scan"+file+"Output/pscan_"+alpha+"/finalResult");
		} catch (IOException e) {
			return;
		}
		Map<String,Set<String>> resMap = new HashMap<>();//记录聚类结果的社区(key为ID,value为社区的内容)
		Map<String,Double> recallMap = new HashMap<>();//记录召回率(key为定义为正例的ID号,value为分类结果的召回率)
		Map<String,Double> precisionMap = new HashMap<>();//记录精确率(key为定义为正例的ID号,value为分类结果的精确率)
		for(String line : ResultCommunity.split("\n")) {//为resMap赋值
			String ID = line.split("\t")[0];
			Set<String> Community = new HashSet<>();
			String tempCommunity = line.split("\t")[1];
			for(String node : tempCommunity.substring(1, tempCommunity.length()-1).split(",")) {
				Community.add(node.trim());
			}
			resMap.put(ID, Community);
		}
		
		int num=0;
		//在多分类的情况下,采用一对多的形式(设定一个为正例,其余均为反例),来计算评价聚类结果的指标
		for(String line : StandardCommunity.split("\n")) {//遍历所有的标准社区集合,计算以每个为正例时的评价指标(召回率,精确率)
			num++;
			Set<String> Community = new HashSet<>();
			String tempCommunity = line.split("\t")[1];
			for(String node : tempCommunity.substring(1,tempCommunity.length()-1).split(",")) {
				Community.add(node.trim());
			}
			
			String tempID = MaxSameCommunity(resMap, Community);//找到与当前标准社区集合相对应的结果社区集合的ID
			Double recall = calRecall(resMap.get(tempID), Community);//计算以此ID为正例社区的召回率
			recallMap.put(String.valueOf(num) + "->" +tempID+" ", recall);
			
			Double precision = calPrecision(resMap.get(tempID), Community);//计算以此ID为正例社区的精确率
			precisionMap.put(String.valueOf(num) + "->" +tempID+" ", precision);
		}
		double FScore = calFScore(recallMap, precisionMap);//通过召回率和精确率来计算F-Score
		String content = "F-Score: " + String.valueOf(FScore) + "\n" + recallMap +"\n"+precisionMap;
		//将计算的F-Score写入HDFS文件当中
		writeFileToHDFS("hdfs://localhost:9000/user/hadoop/scanOutput/scan"+file+"Output/pscan_"+alpha+"/F-Score",content);
	}

}

