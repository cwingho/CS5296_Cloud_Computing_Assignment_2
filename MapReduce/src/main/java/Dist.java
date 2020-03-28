import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
			String word = key.toString();
			
			// add word count to hashmap
			if(hm.containsKey(word)){
				hm.put(word,hm.get(word)+sum);
			}else {
				hm.put(word,sum);
			}	
		}
	}
	
    private static Map<String, Integer> sortByValue(Map<String, Integer> unsortMap)
    {
        List<Entry<String, Integer>> list = new LinkedList<>(unsortMap.entrySet());

        // Sorting the list based on values
        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()) == 0
                ? o1.getKey().compareTo(o2.getKey())
                : o2.getValue().compareTo(o1.getValue()));
        return list.stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue, (a, b) -> b, LinkedHashMap::new));

    }
	
	private static String getWordCnt(String file_name, String file_path) {
		try {
			Map<String,Integer> _hm = new LinkedHashMap<String,Integer>();
			br = new BufferedReader(new FileReader(file_path+"/part-r-00000"));
	        String line = null;
	        
	        while ((line = br.readLine()) != null) {
	        	String[] pair = line.split("	");
	        	_hm.put(pair[0], Integer.parseInt(pair[1]));
	        }
	        
	        _hm = sortByValue(_hm);

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
		hm = sortByValue(hm);
		
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
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, status[i].getPath());
			FileOutputFormat.setOutputPath(job, new Path(output_file));
			
			job.waitForCompletion(true);
			
			// get word count of each input file
			bow[idx] = getWordCnt(file_name, output_file);
			
			System.out.println("Finished: "+file_name);
        }
		
		bw = new BufferedWriter(new FileWriter(output_dir+"output.txt"));
		for(String line: bow) {
			bw.write(line+'\n');
		}
		bw.write(getTotalWordCnt());
		br.close();
		bw.close();
	}

}
