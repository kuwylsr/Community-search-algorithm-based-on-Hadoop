package cs_v1;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
	public String readFileFromHDFS(String filePath) throws IOException, URISyntaxException{
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
}
