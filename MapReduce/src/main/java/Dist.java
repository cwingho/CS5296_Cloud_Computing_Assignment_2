import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

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
	
	private final static int N_TEST_FILE = 10;
	private static BufferedReader br;
	private static BufferedWriter bw;
	private static Map<String,Integer> hm = new LinkedHashMap<String,Integer>();  
	
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

				if(token.length() < 2) {
					continue;
				}
				
				if(token.substring(0, 2).equals("ex")) {
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
	
	// ref: https://stackoverflow.com/questions/8119366/sorting-hashmap-by-values
    private static Map<String, Integer> sort(Map<String, Integer> unsortMap)
    {	
        List<Entry<String, Integer>> list = new LinkedList<>(unsortMap.entrySet());

        // Sorting the list based on values
        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()) == 0
                ? o1.getKey().compareTo(o2.getKey())
                : o2.getValue().compareTo(o1.getValue()));
        return list.stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue, (a, b) -> b, LinkedHashMap::new));

    }
	
	private static String getWordCnt(FileSystem fs, String file_name, String file_path) {
		try {
			Map<String,Integer> _hm = new LinkedHashMap<String,Integer>();
			
			br = new BufferedReader(new InputStreamReader(fs.open(new Path(file_path+"/part-r-00000"))));
	        String line = null;
	        
	        while ((line = br.readLine()) != null) {
	        	String[] pair = line.split("	");
	        	String word = pair[0];
	        	Integer sum = Integer.parseInt(pair[1]);
	        	_hm.put(word, sum);
	        	
	        	// add word count to hashmap
				if(hm.containsKey(word)){
					hm.put(word,hm.get(word)+sum);
				}else {
					hm.put(word,sum);
				}
	        }
	        br.close();
	        
	        _hm = sort(_hm);

	        // output result
	        ArrayList<String> pairs = new ArrayList<String>();
			for(Entry<String, Integer> m: _hm.entrySet()){  
				pairs.add(m.getKey()+", "+String.valueOf(m.getValue()));  
			} 
	        
	        String output = String.join(", ", pairs);
	        output = file_name+" "+output;
	        
	        return output;
	        	        
		} catch (Exception e) {
	        System.err.println(e);
		}
		return "";
	}
	
	private static String getTotalWordCnt() {
		hm = sort(hm);
		
		// output result
        ArrayList<String> pairs = new ArrayList<String>();
		for(Entry<String, Integer> m: hm.entrySet()){  
			pairs.add(m.getKey()+", "+String.valueOf(m.getValue()));  
		} 
        
        String output = String.join(", ", pairs);
        output = "Total "+output;
        
        return output;
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if (args.length != 2) {
			System.err.println("Usage: dist <in> <out>");
			System.exit(0);
		}
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String[] bow = new String[N_TEST_FILE];
		String file_name = "";
		String input_dir= "";
		String output_dir= "";
		String output_file= "";
		
		if(!args[0].substring(args[0].length() - 1).equals("/")){
			input_dir = args[0]+"/";
		}else {
			input_dir = args[0];
		}
		
		if(!args[1].substring(args[1].length() - 1).equals("/")){
			output_dir = args[1]+"/";
		}else {
			output_dir = args[1];
		}
		
		for (int i=0;i<N_TEST_FILE;i++){
			// create file to store result of each input txt
			file_name = "file"+((i+1 != N_TEST_FILE)?"0":"")+String.valueOf(i+1);
			output_file = output_dir+file_name;
			
			Job job = Job.getInstance(conf, "bow");
		    job.setJarByClass(Dist.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setNumReduceTasks(1);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(input_dir+file_name+".txt"));
			FileOutputFormat.setOutputPath(job, new Path(output_file));
			
			job.waitForCompletion(true);
			
			// get word count of each input file
			bow[i] = getWordCnt(fs, file_name, output_file);
			
			System.out.println("Finished: "+file_name);
        }
		
		// create file on hdfs
		OutputStream fsdos = fs.create(new Path(output_dir+"output.txt"),true);
		
		bw = new BufferedWriter(new OutputStreamWriter(fsdos));
		for(String line: bow) {
			bw.write(line+'\n');
		}
		
		// write total count
		bw.write(getTotalWordCnt());
		
		bw.close();
		fsdos.close();
		fs.close();
	}

}
