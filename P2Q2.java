
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.hadoop.filecache.DistributedCache;

import java.io.*;
import java.net.URI;

@SuppressWarnings("deprecation")
public class P2Q2 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {	
		 
		private HashMap<String, String> userInfo = new HashMap<String, String>();
	    String movieId;
		
		public void setup(Context context) throws IOException, InterruptedException	{ 
			super.setup(context);
			
			Configuration conf = context.getConfiguration();
			String input = conf.get("movieId");
			movieId = input; 

			Path[] localPaths = context.getLocalCacheFiles();
			
			for(Path myfile:localPaths){
				
				String line = null;
				String nameofFile = myfile.getName().toString().trim();
				
				if(nameofFile.equals("users.dat")) {          //file name!!
					
					File file = new File(nameofFile);
					FileReader fr = new FileReader(file);
					BufferedReader br = new BufferedReader(fr);
					line = br.readLine();

					while(line != null){
					String[] parts = line.split("::");
					userInfo.put(parts[0], parts[1]+" " +parts[2]);
					line = br.readLine();
					}
					br.close();	
				}
		 			
			}
	

		
		}
		
			 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] arr = line.split("::");
//			String userId;
			if(arr[1].equals(movieId) && Integer.parseInt(arr[2])>=4) {
				//userId = arr[0];
				context.write(new Text(arr[0]),new Text(userInfo.get(arr[0])));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		private Text out = new Text();
		public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
			
			context.write(key, out);
		}
	}	 
		        
	public static void main(String[] args) throws Exception {	
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("Usage: InMemoryJoin <input: ratings.dat> <out> <MovieId>");
			System.exit(2);
		}
		
		conf.set("movieId", otherArgs[2]);

		Job job = new Job(conf, "InMemoryJoin");
		
		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		
   		job.addCacheFile(new URI(NAME_NODE + "/user/hue/users/users.dat"));

		job.setJarByClass(P2Q2.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		job.waitForCompletion(true);

	}

}
