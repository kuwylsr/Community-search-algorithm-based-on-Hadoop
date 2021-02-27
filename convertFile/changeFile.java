package convertFile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class changeFile {

	public void convertFile(String filePath) throws IOException {
		FileInputStream inputStream = new FileInputStream(filePath);
		FileOutputStream outputStream = new FileOutputStream("src/v1/cpmExperiment.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));
		String line = "";
		String temp = "";
		ArrayList<String> list = new ArrayList<>();
		while ((line = br.readLine()) != null) {
			if (line.contains("#")) {
				continue;
			}
			String[] content = line.split("\t");
			if (!list.contains(content[0])) {
				list.add(content[0]);
				if (!temp.equals("")) {
					temp = temp + '\n';
				}
				temp = temp + content[0] + ":" + content[1];
			} else {
				temp = temp + "," + content[1];
			}
		}
		bw.write(temp);
		bw.flush();
		bw.close();
		br.close();
	}

	/**
	 * 改变输入文件的格式
	 * @param filePath
	 * @throws Exception
	 */
	public static void convertFileMap(String filePath) throws Exception {
		FileInputStream inputStream = new FileInputStream(filePath);
		FileOutputStream outputStream = new FileOutputStream("src/cd_v1/cpmScience.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));
		Map<String, String> map = new HashMap<>();
		String line = "";
		int num = 0;
		while ((line = br.readLine()) != null) {
			num++;
			if (num % 10000 == 0) {
				System.out.println(num);
			}
			if (line.contains("#")) {
				continue;
			}
			String[] content = line.split(" ");
			if (!map.containsKey(content[0])) {
				map.put(content[0], ":" + content[1]);
			} else {
				String temp = map.get(content[0]) + "," + content[1];
				map.put(content[0], temp);
			}
			if (!map.containsKey(content[1])) {
				map.put(content[1], ":" + content[0]);
			} else {
				String temp = map.get(content[1]) + "," + content[0];
				map.put(content[1], temp);
			}
		}
		for (Map.Entry<String, String> entry : map.entrySet()) {
			bw.write(entry.getKey() + entry.getValue() + '\n');
		}
		bw.flush();
		bw.close();
		br.close();
	}

	/**
	 * 改变标准社区的格式
	 * @param filePath
	 * @throws Exception
	 */
	public static void convertStandardFileMap(String filePath) throws Exception {
		FileInputStream inputStream = new FileInputStream(filePath);
		FileOutputStream outputStream = new FileOutputStream("src/TestFile/StandardCommunityScience.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));
		Map<String, Set<String>> map = new HashMap<>();
		String line = "";
		int num = 0;
		while ((line = br.readLine()) != null) {
			num++;
			String[] content = line.split(",");
			if (!map.containsKey(content[1])) {
				Set<String> temp = new HashSet<>();
				temp.add(content[0]);
				map.put(content[1], temp);
			} else {
				Set<String> temp = map.get(content[1]);
				temp.add(content[0]);
				map.put(content[1], temp);
			}
		}
		for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
			bw.write(entry.getKey() + "\t" + entry.getValue() + "\n");
		}
		bw.flush();
		bw.close();
		br.close();
	}

	public static void main(String[] args) throws Exception {
		convertFileMap("src/TestFile/StandardScience.txt");
		// convertStandardFileMap("src/TestFile/StandardScience.txt");
	}

}
