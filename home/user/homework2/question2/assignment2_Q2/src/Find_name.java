import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeMap;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;


public class Find_name{			
	
	private static class CombineEntity implements WritableComparable<CombineEntity>{
		
		private Text joinKey;
		private Text flag;
		private Text secondPart;	
		
		public CombineEntity() {
		
			this.joinKey=new Text();
			this.flag=new Text();
			this.secondPart=new Text();
		}
		
		public Text getJoinKey() {
			return joinKey;
		}
		public void setJoinKey(Text joinKey) {
			this.joinKey = joinKey;
		}
		public Text getFlag() {
			return flag;
		}
		public void setFlag(Text flag) {
			this.flag = flag;
		}
		public Text getSecondPart() {
			return secondPart;
		}
		public void setSecondPart(Text secondPart) {
			this.secondPart = secondPart;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.joinKey.readFields(in);
			this.flag.readFields(in);
			this.secondPart.readFields(in);			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			this.joinKey.write(out);
			this.flag.write(out);
			this.secondPart.write(out);			
		}

		@Override
		public int compareTo(CombineEntity o) {
			return this.joinKey.compareTo(o.joinKey);
		}		
	}
	
	private static class JMapper extends Mapper<LongWritable, Text, Text, CombineEntity>{
	
		private CombineEntity combine=new CombineEntity();
		private Text flag=new Text();
		private  Text joinKey=new Text();
		private Text secondPart=new Text();						
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {					
			  
            String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
		
            if(pathName.endsWith("movies.dat")){            	
            	String  valueItems[]=value.toString().split("::");
            //MovieID::Title::Genres
            	flag.set("0");               	
            
            	joinKey.set(valueItems[0]);      
            
            	secondPart.set(valueItems[1]);            	
            	
            	combine.setFlag(flag);
            	combine.setJoinKey(joinKey);
            	combine.setSecondPart(secondPart);            	
            
            	context.write(combine.getJoinKey(), combine);            	            	
            }else if(pathName.endsWith("ratings.dat")){ 
			//UserID::MovieID::Rating::Timestamp
            	String  valueItems[]=value.toString().split("::");
            	
            	flag.set("1");               	
            	
            	joinKey.set(valueItems[1]);      
           
            	secondPart.set(valueItems[2]);            	
            
            	combine.setFlag(flag);
            	combine.setJoinKey(joinKey);
            	combine.setSecondPart(secondPart);
            	context.write(combine.getJoinKey(), combine);            	            	
            }		 			
		}		
	}		
	private static class JReduce extends Reducer<Text, CombineEntity, Text, Text>{				
		
		private List<Text> leftTable=new ArrayList<Text>();
		
		private List<Text> rightTable=new ArrayList<Text>();		
		private Text secondPart=null;		
		private Text output=new Text();						 
		
		@Override
		protected void reduce(Text key, Iterable<CombineEntity> values,Context context)
				throws IOException, InterruptedException {
			 leftTable.clear();
			 rightTable.clear();			 			 		 
			 for(CombineEntity ce:values){
				 
				 this.secondPart=new Text(ce.getSecondPart().toString());				 				 	 
				 if(ce.getFlag().toString().trim().equals("0")){
					 leftTable.add(secondPart);					 
				 }else if(ce.getFlag().toString().trim().equals("1")){					 
					 rightTable.add(secondPart);					 
				 }				 
			 }

			 for(Text left:leftTable){				 
				 for(Text right:rightTable){					 
					 output.set(left+"\t"+right);
					 context.write(key, output);
				 }				 
			 }			
		}		
	}
		
	public static class AverageMap extends Mapper<LongWritable, Text, Text, Text> {
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	String line = value.toString();
	String[] lineSplit = line.split("\t");
	String MovieId = lineSplit[0];
	String MovieName = lineSplit[1];
	String Rating = lineSplit[2];
	context.write(new Text(MovieId), new Text(Rating +"\t"+MovieName+"\t"+ "1")); 																														
}
}
	
  public static class AverageReduce extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		int cnt = 0;
		String MovieName = null;
		for (Text val : values) {
			String[] str = val.toString().split("\t");
			sum += Integer.parseInt(str[0]);
			MovieName = str[1];
			cnt += Integer.parseInt(str[2]);
		}
		double res = (sum * 1.0) / cnt;
		context.write(key, new Text( Double.toString(res)+"\t"+MovieName));
	}
}

public static class SortMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	public DoubleWritable doublerate = new DoubleWritable();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] lineSplit = line.split("\t");
		String MovieId = lineSplit[0];
		String Rating = lineSplit[1];	
		String MovieName = lineSplit[2];
		if(lineSplit[0]!=null){
			doublerate.set(Double.parseDouble(Rating));
			value.set(lineSplit[0]+"\t"+MovieName);
			context.write(doublerate, value);
		}
	}
}

public static class SortReduce extends
		Reducer<DoubleWritable, Text,Text, DoubleWritable> {
	int count=0;
	public void reduce(DoubleWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
		for (Text value : values) {
			if(count < 10){
				count++;
				context.write(value, key);
			}
		}			
	}
}	

	
	 public static class DescSort extends  WritableComparator{

	       public DescSort() {
	         super(IntWritable.class,true);
	      }
	       @Override
	       public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
	          int arg4, int arg5) {
	        return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);
	      }
	       
	       @Override
	      public int compare(Object a, Object b) {
	     
	        return   -super.compare(a, b);
	      }	      
	    }
		
	/* 
		 Configuration conf = new Configuration();			  		  
		 Job job = new Job(conf, "Find_name");
		 job.setJarByClass(Find_name.class);
		 job.setMapperClass(JMapper.class);
		 job.setReducerClass(JReduce.class);		 
	
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(CombineEntity.class);		 
		
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);		 	
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputFormatClass(TextOutputFormat.class);		 	 
		 FileInputFormat.addInputPath(job, new Path(args[0]));  
         FileInputFormat.addInputPath(job, new Path(args[1]));  
	     FileOutputFormat.setOutputPath(job, new Path(args[2]));   		 
	 */
	@SuppressWarnings({ "deprecation", "deprecation", "deprecation" })
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Find_name.class);
		conf.set("mapred.job.tracker", "127.0.0.1:9001");
		//conf.setJar("Find_name.jar");

		Job job1 = new Job(conf, "Join1");
		job1.setJarByClass(Find_name.class);
		job1.setMapperClass(JMapper.class);
		job1.setReducerClass(JReduce.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(CombineEntity.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		ControlledJob ctrljob1 = new ControlledJob(conf);
		ctrljob1.setJob(job1);  
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));

		Job job2 = new Job(conf, "Join2");
		job2.setJarByClass(Find_name.class);
		job2.setMapperClass(AverageMap.class);
		job2.setReducerClass(AverageReduce.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		ControlledJob ctrljob2 = new ControlledJob(conf);
		ctrljob2.setJob(job2);
		ctrljob2.addDependingJob(ctrljob1);// this is the connection with previous
		FileInputFormat.addInputPath(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));

		Job job3 = new Job(conf, "Join3");
		job3.setJarByClass(Find_name.class);
		job3.setMapperClass(SortMap.class);
		job3.setReducerClass(SortReduce.class);
		job3.setSortComparatorClass(DescSort.class);
		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);

		ControlledJob ctrljob3 = new ControlledJob(conf);
		ctrljob3.setJob(job3);
		ctrljob3.addDependingJob(ctrljob2);// this is the connection with previous
		FileInputFormat.addInputPath(job3, new Path(args[3]));
		FileOutputFormat.setOutputPath(job3, new Path(args[4]));
		
		JobControl jobCtrl = new JobControl("myctrl");
		jobCtrl.addJob(ctrljob1);
		jobCtrl.addJob(ctrljob2);
		jobCtrl.addJob(ctrljob3);

		Thread t = new Thread(jobCtrl);
		t.start();

		while (true) {

			if (jobCtrl.allFinished()) {
				System.out.println(jobCtrl.getSuccessfulJobList());
				jobCtrl.stop();
				break;
			}
			if (jobCtrl.getFailedJobList().size() > 0) {
				System.out.println(jobCtrl.getFailedJobList());
				jobCtrl.stop();
				break;
			}
		}
	}
}
