import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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

public class Bow {
	private final static String[] top100Word = { "the", "be", "to", "of", "and", "a", "in", "that", "have", "i", 
			"it", "for", "not", "on", "with", "he", "as", "you", "do", "at", "this", "but", "his", "by", "from", 
			"they", "we", "say", "her", "she", "or", "an", "will", "my", "one", "all", "would", "there", "their", 
			"what", "so", "up", "out", "if", "about", "who", "get", "which", "go", "me", "when", "make", "can", 
			"like", "time", "no", "just", "him", "know", "take", "people", "into", "year", "your", "good", "some", 
			"could", "them", "see", "other", "than", "then", "now", "look", "only", "come", "its", "over", "think", 
			"also", "back", "after", "use", "two", "how", "our", "work", "first", "well", "way", "even", "new", 
			"want", "because", "any", "these", "give", "day", "most", "us" };
	
	private static BufferedReader br;
	private static BufferedWriter bw;
	private static LinkedHashMap<String,String> hm = new LinkedHashMap<String,String>();  
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException{
			
			// filter special character and convert to lower case
			String clean_str = value.toString().replaceAll("[^a-zA-Z0-9]", " ");
			clean_str = clean_str.toLowerCase();
			
			StringTokenizer itr = new StringTokenizer(clean_str);
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				
				// if token is in top 100 words
				if(Arrays.asList(top100Word).contains(token)){
					word.set(token);
					context.write(word, one);
				}
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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
	
	private static String getBow(FileSystem fs, String file_name, String file_path) {
		try {
			FileStatus[] status = fs.listStatus(new Path(file_path));
			
			// read all file
			for (int i=0;i<status.length;i++){	
				
				br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
		        String line = null;
		        
		        while ((line = br.readLine()) != null) {
		        	String[] pair = line.split("	");
		        	hm.put(pair[0], pair[1]);
		        }
		        br.close();
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
			System.err.println("Argument: <in> <out>");
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
			// init hashmap for every input file
			for(String word: top100Word) {
				hm.put(word,"0"); 
			}
			
			// create file to store result of each input txt
			file_name = status[i].getPath().getName();
			file_name = file_name.substring(0, file_name.lastIndexOf('.'));
			output_file = output_dir+file_name;
			
			// get file index
			int idx = Integer.parseInt(file_name.replaceAll("\\D+",""))-1;
			
			Job job = Job.getInstance(conf, "bow");
		    job.setJarByClass(Bow.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, status[i].getPath());
			FileOutputFormat.setOutputPath(job, new Path(output_file));
			
			job.waitForCompletion(true);
			
			// get bag of words
			bow[idx] = getBow(fs, file_name, output_file);
			
			System.out.println("Finished: "+file_name);
        }

		// create file on hdfs
		OutputStream fsdos = fs.create(new Path(output_dir+"output.txt"),true);
		
		bw = new BufferedWriter(new OutputStreamWriter(fsdos));
		for(String line: bow) {
			bw.write(line+'\n');
		}
		
		bw.close();
		fsdos.close();
		fs.close();
	}
}
