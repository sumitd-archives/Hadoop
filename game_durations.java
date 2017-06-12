import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.commons.io.FileUtils;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessGamePartC {
    public static enum Counter{
        recordCount 
    };
    public static class Mapper1 extends Mapper<Object, Text, LongWritable, IntWritable>{

        public Object lock = new Object();
        private Text word = new Text();
        private IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString() , "\r\n");
            String line = null;
            while (itr.hasMoreTokens()) {
                line = itr.nextToken();
                line = line.substring(1 , line.length() -1);
                String[] words = line.split(" ");
                if (words[0].equals("PlyCount")) {//find the value of plycount
                    String[] result = words[1].substring(1).split("-");
                    String plycount = words[1].substring(1 , words[1].length() -1);
                    Long duration = Long.parseLong(plycount , 10);
                    context.write(new LongWritable(duration) , one);
                    context.getCounter(Counter.recordCount).increment(1);
                }
            }
    	}
    }

    public static class Mapper2 extends 
        Mapper<Object, Text, LongWritable, IntWritable>{

        public Object lock = new Object();
        private Text word = new Text();
        private IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) 
               throws IOException, InterruptedException {
            //read the temporary output of the first reducer
            StringTokenizer itr = new StringTokenizer(value.toString(), "\r\n");
            String line = null;
            while (itr.hasMoreTokens()) {
                line = itr.nextToken();
                String[] words = line.split("\\s+");
                Integer duration = Integer.parseInt(words[1]);
                Long count = Long.parseLong(words[0] , 10);
                //the key,value of the first reducer output
                //gets interchanged . 
                context.write(new LongWritable(count) , 
                              new IntWritable(duration));
            }
        }
    }

    public static class Reducer1 extends Reducer<LongWritable,IntWritable,LongWritable,LongWritable> {

	    public void reduce( LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum =0;
	        for (IntWritable val : values) {
	            sum += (long)val.get();
	        }
            //write the <duration , #games> as the output
            context.write(new LongWritable(sum) , key);
        } 
    }

    public static class Reducer2 extends Reducer<LongWritable,IntWritable,IntWritable,Text> {

	    public void reduce( LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            float percent = 0;
            Configuration conf = context.getConfiguration();
            //get the value from the configuration
            long total = conf.getLong("total" , -1);
            percent = (float)key.get() / total * 100;
            String symbol = "%";
            //format the output
            String value = String.format("%1.2f" , percent) + symbol;
	        for (IntWritable outputkey : values) {
                context.write(outputkey , new Text(value));
	        }
        } 
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "phase1");
        job.setJarByClass(ChessGamePartC.class);
        job.setMapperClass(Mapper1.class);
        //the intermediate sorting should be decreasing
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("reducer1_output"));
        job.waitForCompletion(true); 
        //get the value of the counter incremented in first mapper
        Counters counters = job.getCounters();
        long total = counters.findCounter(Counter.recordCount).getValue();

        //the second map reduce phase    
        Configuration conf2 = new Configuration();
        //store the value of the counter from first mapper in configuration
        conf2.setLong("total" , total);
        Job job2 = Job.getInstance(conf2, "phase2");
        job2.setJarByClass(ChessGamePartC.class);
        job2.setMapperClass(Mapper2.class);
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("reducer1_output"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);
        FileUtils.deleteDirectory(new File("reducer1_output"));
    }
}
