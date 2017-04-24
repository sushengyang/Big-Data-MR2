import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class hw2p1{
	
	class ValueComparator implements Comparator<String> {
	 
    Map<String, Double> map;
 
    public ValueComparator(Map<String, Double> base) {
        this.map = base;
    }
 
    public int compare(String a, String b) {
        if (map.get(a) >= map.get(b)) {
            return -1;
        } else {
            return 1;
        } 
    }
}
	
	public static TreeMap<String, Double> SortByValue (HashMap<String, Double> map) {
		ValueComparator vc =  new ValueComparator(map);
		TreeMap<String,Double> sortedMap = new TreeMap<String,Double>(vc);
		sortedMap.putAll(map);
		return sortedMap;
	}
	
	
	public static class Map1 extends Mapper<LongWritable,Text,Text,Text>{
		private Text userid = new Text();
		private Text gender = new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String[] mydata = value.toString().split("::");
			userid.set(mydata[0]);
			gender.set("U"+mydata[1]);
			if(gender.toString().substring(1).equals("F"))
				context.write(userid,gender);
		}
		
	}
	public static class Map2 extends Mapper<LongWritable,Text,Text,Text>{
		private Text userid = new Text();
		private Text movierating = new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String[] mydata = value.toString().split("::");
			userid.set(mydata[0]);
			movierating.set("R"+mydata[1]+"\t"+mydata[2]);
			context.write(userid, movierating);
		}
	}
	
	public static class Reduce1 extends Reducer<Text,Text,Text,Text>{
		List<Text> ListU = new ArrayList<Text>();
		List<Text> ListR = new ArrayList<Text>();
		private Text result = new Text();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			ListU.clear();
			ListR.clear();
			for(Text val:values){
				if (val.toString().charAt(0) == 'U')
					ListU.add(new Text(val.toString().substring(1)));
				else if (val.toString().charAt(0) == 'R')
					ListR.add(new Text(val.toString().substring(1)));
			}
				for(Text A:ListU)
					for(Text B:ListR){
						result.set(B.toString());
						context.write(key,result);
					}

			}
		}
	public static class Map3 extends Mapper<LongWritable,Text,Text,DoubleWritable>{
		private Text movieid = new Text();
		private DoubleWritable ratings = new DoubleWritable();
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String[] mydata = value.toString().split("\t");
			movieid.set(mydata[1]);
			ratings.set(Double.parseDouble(mydata[2]));
			context.write(movieid,ratings);
		}
	}
	
	public static class Reduce2 extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
		
		private DoubleWritable averatings = new DoubleWritable();
		public void reduce(Text key,Iterable<DoubleWritable> values,Context context) throws IOException,InterruptedException{
			 int num = 0;
			 double sum = 0;
			 double ave = 0;
			
			for(DoubleWritable rating:values){
				sum += rating.get();
				num += 1;	  
			}
			ave = sum/num;
			averatings.set(ave);
			context.write(key,averatings);
		}
	}
	
	public static class Map4 extends Mapper<LongWritable,Text,NullWritable,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			
			context.write(NullWritable.get(),value);
			
		}
	}
	
	public static class Reduce3 extends Reducer<NullWritable,Text,Text,DoubleWritable>{
		public void reduce(NullWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			   HashMap<String,Double> result = new HashMap<String,Double>();

				for(Text value:values){
					String[] data = value.toString().split("\t");
					result.put(data[0], Double.parseDouble(data[1]));

				}
				
			TreeMap<String,Double> sorted = SortByValue(result);
			int i = 0;
			for(String k:sorted.keySet()){
				if(i<5){
				context.write(new Text(k),new DoubleWritable(result.get(k)));
				i++;
				}
			
			}
					
			}
	}
	
	
	public static class Map5 extends Mapper<LongWritable,Text,Text,Text>{
		private Text movieid = new Text();
		private Text rating = new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String[] mydata = value.toString().split("\t");
			movieid.set(mydata[0]);
			rating.set("A"+mydata[1]);
			context.write(movieid, rating);
		}
	}
	
	public static class Map6 extends Mapper<LongWritable,Text,Text,Text>{
		private Text movieid = new Text();
		private Text movietitle = new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String[] mydata = value.toString().split("::");
			movieid.set(mydata[0]);
			movietitle.set("B"+mydata[1]);
			context.write(movieid, movietitle);
		}
	}
	
	public static class Reduce4 extends Reducer<Text,Text,Text,Text>{
		List<Text> ListA = new ArrayList<Text>();
		List<Text> ListB = new ArrayList<Text>();
		private Text result = new Text();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			ListA.clear();
			ListB.clear();
			for(Text val:values){
				if (val.toString().charAt(0) == 'A')
					ListA.add(new Text(val.toString().substring(1)));
				else if (val.toString().charAt(0) == 'B')
					ListB.add(new Text(val.toString().substring(1)));
			}
				for(Text A:ListA)
					for(Text B:ListB){
						result.set(B.toString());
						context.write(B,A);
					}

			}
		}
	
	
	
	
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 7) {
			System.err.println("Usage: hw2p1 <users> <ratings> <movies> <out1> <out2> <out3> <out4>");
			System.exit(2);
		}
		Job job1 = new Job(conf, "hw2p1j1"); 
		Job job2 = new Job(conf,"hw2p1j2");
		
		job1.setJarByClass(hw2p1.class);
		job1.setReducerClass(Reduce1.class);
		MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class,Map1.class );
		MultipleInputs.addInputPath(job1, new Path(otherArgs[1]),TextInputFormat.class,Map2.class );
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]));
		job2.setJarByClass(hw2p1.class);
		job2.setReducerClass(Reduce2.class);
		job2.setMapperClass(Map3.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		Job job3 = new Job(conf,"hw2p1j3");
		job3.setJarByClass(hw2p1.class);
		job3.setMapperClass(Map4.class);
		job3.setReducerClass(Reduce3.class);
		FileInputFormat.addInputPath(job3, new Path(otherArgs[4]));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[5]));

		job3.setMapOutputKeyClass(NullWritable.class);
		job3.setMapOutputValueClass(Text.class);
		
		Job job4 = new Job(conf,"hw2p1j4");
		job4.setJarByClass(hw2p1.class);
		job4.setReducerClass(Reduce4.class);
		MultipleInputs.addInputPath(job4, new Path(otherArgs[5]), TextInputFormat.class,Map5.class );
		MultipleInputs.addInputPath(job4, new Path(otherArgs[2]),TextInputFormat.class,Map6.class );
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job4, new Path(otherArgs[6]));
	
		if(job1.waitForCompletion(true)){
			job2.waitForCompletion(true);
			if(job2.waitForCompletion(true)){
				job3.waitForCompletion(true);
				if(job3.waitForCompletion(true))
					job4.waitForCompletion(true);
			}
		}
	}
	

}


