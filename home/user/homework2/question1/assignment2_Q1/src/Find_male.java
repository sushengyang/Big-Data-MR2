import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.net.URI;
 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
@SuppressWarnings("deprecation")
public class Find_male extends Configured implements Tool {
    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
 
        // 用于缓存 sex、user 文件中的数据
        private HashMap<String, String> userMap = new HashMap<String, String>();
      
        private Text oKey = new Text();
        private Text oValue = new Text();
        private String[] kv;
 
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException 
                {      
        	
        	//System.out.println("in mapper setup: " + context.getConfiguration().get("name"));
           Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p : files){							
				BufferedReader br = new BufferedReader(new FileReader(new File(p.getName().toString())));
				String str = null;
				while((str = br.readLine())!=null){
					String[] line = str.split("::");
					if(line.length != 0) {
						userMap.put(line[0].trim(), line[1].trim()); 
					}
				}
				br.close();		
			}
          }
   
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
        	//System.out.println("in mapper : " + context.getConfiguration().get("name"));
            kv = value.toString().split("::");
            if (userMap.containsKey(kv[0])  ){
                oKey.set(kv[1].toString());
             //   oValue.set(userMap.get(kv[0])+"\t"+kv[0].toString());
                oValue.set(userMap.get(kv[0]));
                context.write(oKey, oValue);
            }
        }
 
    }
 
    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {
 
       private Text oValue = new Text();
        private String name;      
        public void setup(Context context) {
            this.name= context.getConfiguration().get("name");
            //System.out.println("in reduce setup: " + this.name);
        }
 
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        	String valueString;
            String out = "";
             int sum =0;
            for (Text value : values) {
            	
                valueString = value.toString();
                if(valueString.equals("M"))
                {
              //  out += valueString + "|";
                	sum = sum+1;
                }
              }
           String name = context.getConfiguration().get("name");
       //    System.out.println("reduce: " + name);
	            String movieID = key.toString();
	           
	             if(movieID.equals(name))
	             {
	            	   oValue.set(String.valueOf(sum));
	                   context.write(oValue, NullWritable.get() );
	             }
           // oValue.set(out);
           // oValue.set(String.valueOf(sum));
           // context.write(key, oValue);
        }
 
    }
 
    public int run(String[] args) throws Exception {
       
        Configuration conf = new Configuration();
        conf.set("name", args[3]);
        //System.out.println("arg3: " + args[3]);
        Job job = new Job(conf, "MultiTableJoin");
       DistributedCache.addCacheFile(new URI(args[0]),job
               .getConfiguration());
	
       job.setJobName("MultiTableJoin");
       job.setJarByClass(Find_male.class);
       job.setMapperClass(MapClass.class);
       job.setReducerClass(Reduce.class);      
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);

       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);
       FileInputFormat.addInputPath(job, new Path(args[1]));
       FileOutputFormat.setOutputPath(job, new Path(args[2]));
       
       job.waitForCompletion(true);
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Find_male(),
                args);
        System.exit(res);
    }
 
}
 
