import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
	static public String searchString = "";
	static public String[] searchWords;
	//private String removeChar = "[^a-zA-Z-]";
	public static class InvertedIndexMapper extends Mapper<Object , Text, Text, Text> {
		// 统计词频时，需要去掉标点符号等符号，此处定义表达式
		private String pattern = "[^a-zA-Z0-9-]";
		// 提取文件切片信息
		private FileSplit split;
		// 文件路径
		private String path;
		// 文件名
		private String filename = new String("");
		// key-value 的 value
		private Text valueinfo = new Text();
		//记录段落
		private Long linenum = (long)1;
	    	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// 将每一行转化为一个String
			String line = value.toString();
			// 获取文件信息
			split = (FileSplit) context.getInputSplit();
			// 获取文件路径
			path = split.getPath().toString();
			// 获取文件名
			String file_name = path.substring(path.lastIndexOf("/")+1);
			// 将标点符号等字符用空格替换，这样仅剩单词
			line = line.replaceAll(pattern, " ");	
			// 将String划分为一个个的单词
			String[] words = line.split("\\s+");
			if(file_name.equals(filename)) 
				++linenum;	
			else{
				linenum = (long)1;
				filename = file_name;
			}	
			Long wordnum = (long)0;
			for (String word : words) {
			    if (word.length() > 0) {
				++wordnum;
				if(!matchWord(word.toLowerCase()))
					continue;
				// 设置键对应的值为1,并记录位置
				valueinfo.set("1,"+ linenum.toString()
						+" "+wordnum.toString());
				context.write(new Text(word+":"+filename), valueinfo);
			    }
			}
	    	}
	}

	public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
		// info存储形如(文件名，单词数量)的字符串
		private Text info = new Text();
		//private String filename = "";
		//List<String> list = new ArrayList<String>();
		//String posSetString = "";
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Integer count = 0;
			String posSetString = "";
			// 分离map输出的“key”中的真实key和文件名
                        int sindex = key.toString().indexOf(":");
			/*String file_name = key.toString().substring(sindex+1);
			if(!file_name.equals(filename)){
				filename = file_name;
				list.clear();
				posSetString = "";
			}*/
			List<String> list = new ArrayList<String>();
			for (Text value : values) {
				//获取字符串
				String valueString = value.toString();
				//将单词位置分离出来
				int posindex = valueString.indexOf(",");
				//单词位置组合
				//posSetString += valueString.substring(posindex+1);
				list.add(valueString.substring(posindex+1));
				// 将value转成字符串再提取该值
				count += Integer.parseInt(valueString.substring(0,posindex));
			}
			list.sort(new Comparator<String>() {
				@Override
				public int compare(String arg0,String arg1) {
					Scanner in0 = new Scanner(arg0);
					Scanner in1 = new Scanner(arg1);
					int a = in0.nextInt();
					int b = in1.nextInt();
					if(a != b) return a - b;
					else return in0.nextInt() - in1.nextInt();
				}
			});
			for(String string : list) 
				posSetString += ("[" + string + "]");
			// 分离map输出的“key”中的真实key和文件名
			//int sindex = key.toString().indexOf(":");
			// 将value改成文件名和单词数量对
			info.set("("+key.toString().substring(sindex+1) + ":" +
				count + " " + posSetString +")" );
			// 修改key，改成单词
			key.set( key.toString().substring(0,sindex));
			context.write(key, info);
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
		// 存储一个单词以及(文件名,单词数量,单词位置)列表
		private Text result = new Text();
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//列表
			Integer amount = 0;
			List<String> list = new ArrayList<String>();
			String filelist = new String();
			for (Text value : values) {
				++amount;
				//filelist += value.toString()+";\n";
				list.add(value.toString());
			}
			list.sort(new Comparator<String>() {
                                @Override
                                public int compare(String arg0,String arg1) {
					int index0 = arg0.indexOf(":");
					String str0 = arg0.substring(index0+1);
					int index1 = arg1.indexOf(":");
                                        String str1 = arg1.substring(index1+1);
                                        Scanner in0 = new Scanner(str0);
                                        Scanner in1 = new Scanner(str1);
                                        int a = in0.nextInt();
                                        int b = in1.nextInt();
					if(a != b) return b - a;
					else return arg0.compareTo(arg1);
                                }
                        });
			for(String string : list)
                                filelist += (string + ";\n");
			result.set(amount.toString() + (amount == 1 ? " file" : " files")
				       + " have this word:\n" + filelist);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		// 创建配置对象
		Configuration conf = new Configuration();
		// 创建Job对象
		Job job = Job.getInstance(conf, "invertedindex");
		// 设置运行Job的类
		job.setJarByClass(InvertedIndex.class);
		// 设置Mapper类
		job.setMapperClass(InvertedIndexMapper.class);
		// 设置Map输出的Key value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// 设置Reducer类
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);
		// 设置Reduce输出的Key value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 设置输入输出的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		String removeChar = "[^a-zA-Z]";
		Scanner in = new Scanner(System.in);		
		searchString = in.nextLine().trim();
		searchString = searchString.toLowerCase().replaceAll(removeChar, " ");
                        // 将String划分为一个个的单词
                searchWords = searchString.split("\\s+");
		// 提交job
		boolean b = job.waitForCompletion(true);
		if(!b) {
			System.out.println("InvertedIndex task fail!");
		}

	}

	public static boolean matchWord(String word){
		for(int i=0;i<searchWords.length;++i){
			if( word.indexOf(searchWords[i]) == -1 ) return false;
		}
		return true;
	}
}
