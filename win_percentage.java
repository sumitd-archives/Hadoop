import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessGamePartA {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable>{
        public static Object lock = new Object();
        public static enum MyCounters { TOTAL };
        private final static LongWritable one = new LongWritable(1);
        private final static String white = new String("white");
        private final static String black = new String("black");
        private final static String draw = new String("draw");

        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString() , "\r\n");
            String line = null;
            while (itr.hasMoreTokens()) {
                line = itr.nextToken().substring(1);
                String[] words = line.split(" ");
                //find out whether a black win or a white win or a draw
                //for every game
                if (words[0].equals("Result")) {
                    String[] result = words[1].substring(1).split("-");
                    if (result[0].equals("1")) {
                        context.write(new Text(white) , one);
                    }
                    else if (result[0].equals("0")) {
                        context.write(new Text(black) , one);
                    }
                    else if (result[0].equals("1/2")) {
                        context.write(new Text(draw) , one);
                    }
                }
            }
    	}
    }

    public static class PercentageReducer extends Reducer<Text,LongWritable,Text,Text> {
	    private FloatWritable result = new FloatWritable();
        private final static String white = new String("white");
        private final static String black = new String("black");
        private final static String draw = new String("draw");
        private static long total = 0;
        private static long black_total = 0;
        private static long white_total = 0;
        private static long draw_total = 0;
        private static int attribute_count = 0;

	    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
	        long sum = 0;
            //calculate the total white wins , black wins
            //and draws
	        for (LongWritable val : values) {
	            sum += val.get();
	        }
            if (key.toString().equals(white)) {
                white_total += sum;
            }
            else if (key.toString().equals(black)) {
                black_total += sum;
            }
            else {
                draw_total += sum;
            }
            
            attribute_count += 1;
            //the total count of black wins , white wins 
            //and draws have been established.
            //start writing the output
            if (attribute_count == 3) {
                total = black_total + white_total + draw_total;
                for (int i = 0 ; i < 3 ; i++) {
                //Start writing to output 
                    String attribute = new String();
                    long local_count = 0;
                    if (i == 0) {
                        attribute = white;
                        local_count = white_total;
                    }
                    else if (i == 1) {
                        attribute = black;
                        local_count = black_total;
                    }
                    else {
                        attribute = draw;
                        local_count = draw_total;
                    }

                    String percent = String.format("%1.2f" , (double)local_count / total);
                    String space = "\t";
                    String outputValue = local_count + space + percent;
                    context.write(new Text(attribute), new Text(outputValue));
                }
            }
        } 
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ChessGameA");
        job.setJarByClass(ChessGamePartA.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(PercentageReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
