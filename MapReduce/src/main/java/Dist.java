import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Dist {
	
	private static BufferedReader br;
	private static BufferedWriter bw;
	private static LinkedHashMap<String,String> hm = new LinkedHashMap<String,String>();  
	
	public static class Map extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException{
			
			// filter special character and convert to lower case
			String clean_str = value.toString().replaceAll("[^a-zA-Z0-9]", " ");
			clean_str = clean_str.toLowerCase();
			
			StringTokenizer itr = new StringTokenizer(clean_str);
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				
				if(token.substring(0, 2).equals("ex")) {
					word.set(token);
					context.write(word, one);
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	private static String getBow(String file_name, String file_path) {
		try {
			br = new BufferedReader(new FileReader(file_path+"/part-r-00000"));
	        String line = null;
	        
	        while ((line = br.readLine()) != null) {
	        	String[] pair = line.split("	");
	        	hm.put(pair[0], pair[1]);
	        }
	        
	        ArrayList<String> values = new ArrayList<String>(hm.values());
	        String output = String.join(", ", values);
	        output = file_name+'	'+output;
	        
	        return output;
	        	        
		} catch (IOException e) {
	        System.err.println("Caught exception while loading result.");
		}
		return "";
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if (args.length != 2) {
			System.err.println("Usage: bow <in> <out>");
			System.exit(0);
		}
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(new Path(args[0]));
		String[] bow = new String[status.length];
		String file_name = "";
		String output_dir= "";
		String output_file= "";
		
		if(!args[1].substring(args[1].length() - 1).equals("/")){
			output_dir = args[1]+"/";
		}else {
			output_dir = args[1];
		}
		
		for (int i=0;i<status.length;i++){
			// create file to store result of each input txt
			file_name = status[i].getPath().getName();
			file_name = file_name.substring(0, file_name.lastIndexOf('.'));
			output_file = output_dir+file_name;
			
			// get file index
			int idx = Integer.parseInt(file_name.replaceAll("\\D+",""))-1;
			
			Job job = Job.getInstance(conf, "bow");
		    job.setJarByClass(Bow.class);
			job.setMapperClass(Map.class);
			job.setCombinerClass(Reduce.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, status[i].getPath());
			FileOutputFormat.setOutputPath(job, new Path(output_file));
			
			job.waitForCompletion(true);
			
			// get bag of words
			bow[idx] = getBow(file_name, output_file);
			
			System.out.println("Finished: "+file_name);
        }
		
		bw = new BufferedWriter(new FileWriter(output_dir+"output.txt"));
		for(String line: bow) {
			bw.write(line+'\n');
		}
		
		br.close();
		bw.close();
	}

}
