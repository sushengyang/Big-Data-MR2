/*  map side join
 * Given the id of a movie, find all users who rated the movie 4 or greater.
 * userids,gender and age 
 * You will input the movie id from command line.
   Use the users.dat and ratings.dat
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
@SuppressWarnings("deprecation")

public class Q2 extends Configured implements Tool {
		//The Mapper classes and  reducer  code
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		private HashMap<String, String> myMap = new HashMap<String, String >();
		private Text userid = new Text();
	    private Text output = new Text();
	    private String[] kv;
	    String mymovieid;
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
				// TODO Auto-generated method stu
		super.setup(context);
		mymovieid = context.getConfiguration().get("movieid"); // to retrieve movieid set in main method
	
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for(Path myfile:files)
		 	{
			String line=null;
			String nameofFile=myfile.getName();
			File file =new File(nameofFile+"" );
			FileReader fr= new FileReader(file);
			BufferedReader br= new BufferedReader(fr);
			line=br.readLine();
			while(line!=null)
				{
				String[] arr=line.split( "::" );//UserID::Gender::Age::Occupation::Zip-code  arr
				myMap.put(arr[0], arr[1]+"\t"+arr[2]); //userid gender age
				line=br.readLine();
				}
			br.close();
		 	}
		}

		//UserID::MovieID::Rating::Timestamp  kv
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			kv = value.toString().split("::");
            if (myMap.containsKey(kv[0]) && kv[1].equals(mymovieid)&& Integer.parseInt(kv[2])>=4  ){
               userid.set(kv[0].toString());
               output.set(myMap.get(kv[0]));//gender&age + rating
               context.write(userid, output); //userid gender age rating
			}
		}
	}

	//The reducer class	
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		
		/*protected void setup(Context context)
				throws IOException, InterruptedException {
				// TODO Auto-generated method stu
		super.setup(context);
		
		}*/
		
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			
			
			for (Text val : values) {
				//String[] value =val.toString().split("::");
				//if(value[2].equals(mymovieid) && Integer.parseInt(value[3])>=4)
				//context.write(key,new Text(value[0]+'\t'+value[1]));
				context.write(key, val);
			}
		}		
	}


	public int run(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Usage: JoinExample <in> <in2> <out> <anymovieid>");
			System.exit(2);
		}


		conf.set("movieid", otherArgs[3]); //setting global data variable for hadoop

		Job job = new Job(conf, "joinexc");
		
		DistributedCache.addCacheFile(new URI(args[0]),job.getConfiguration());
	
		job.setJarByClass(Q2.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);
	
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]) );
		//FileInputFormat.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, Map.class );

		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);

		//set the HDFS path of the input data
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		return job.waitForCompletion(true)?0:1;
		
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Q2(), args );
		System.exit(res);

	}

}
