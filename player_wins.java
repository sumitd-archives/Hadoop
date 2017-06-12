import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessGamePartB {
    public static class ChessGameMapper extends Mapper<Object, Text, Text, CustomResults>{
    public String white_name = new String();
    public String black_name = new String();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString() , "\r\n");
            String line = null;

            while (itr.hasMoreTokens()) {
                line = itr.nextToken();
                line = line.substring(1 , line.length() - 1);
                String[] words = line.split(" ");
                String attr = words[0];

                if (attr.equals("White")) {
                    white_name = words[1].
                             substring(1 , words[1].length()-1);//remove the quotes at start and end

                }
                if (attr.equals("Black")) {
                    black_name = words[1].
                             substring(1 , words[1].length()-1);//remove the quotes at start and end
                }
                //create two user defined objects for the two colors
                CustomResults white_result = new CustomResults(CustomResults.white);
                CustomResults black_result = new CustomResults(CustomResults.black);

                if (attr.equals("Result")) {
                    String[] result = words[1].substring(1).split("-");
                    if (result[0].equals("1")) {
                        //set the parameters w.r.t the color
                        white_result.setGameWon(1);
                        black_result.setGameLost(1);
                    }
                    else if (result[0].equals("0")) {
                        black_result.setGameWon(1);
                        white_result.setGameLost(1);
                    }
                    else if (result[0].equals("1/2")) {
                        white_result.setGameDrawn(1);
                        black_result.setGameDrawn(1);
                    }
                    //player name , user defined object   
                    context.write(new Text(white_name) , white_result);
                    context.write(new Text(black_name) , black_result);
                }
            }
    	}
    }

    public static class ChessGameCombiner extends Reducer<Text,CustomResults,Text,CustomResults> {
        private final static String white = new String("White");
        private final static String black = new String("Black");

	    public void reduce(Text key, Iterable<CustomResults> values, Context context) throws IOException, InterruptedException {
        int black_win = 0;
        int black_draw = 0;
        int black_loss = 0;
        int white_win = 0;
        int white_draw = 0;
        int white_loss = 0;
        //the output of each mapper
        //for every player calculate the 
        //black games won , drawn , lost and
        //white games won ,drawn , lost
	    for (CustomResults val : values) {
	        if (val.getColor().toString().equals(black)) {
                int temp = val.getGamesWon().get();
                black_win += temp;
                temp = val.getGamesLost().get();
                black_loss += temp;
                temp = val.getGamesDrawn().get();
                black_draw += temp;
            }

	        if (val.getColor().toString().equals(white)) {
                int temp = val.getGamesWon().get();
                white_win += temp;
                temp = val.getGamesLost().get();
                white_loss += temp;
                temp = val.getGamesDrawn().get();
                white_draw += temp;
            }
        }
        //create two new objects for every player.
        //one storing the results of the white games of the player
        //the other the black games of the player
        CustomResults white_result = new CustomResults(CustomResults.white);
        CustomResults black_result = new CustomResults(CustomResults.black);

        white_result.setGameWon(white_win);
        white_result.setGameLost(white_loss);
        white_result.setGameDrawn(white_draw);
        context.write(key , white_result);

        black_result.setGameWon(black_win);
        black_result.setGameLost(black_loss);
        black_result.setGameDrawn(black_draw);
        context.write(key , black_result);
      }
    }

    public static class ChessGameReducer extends Reducer<Text,CustomResults,Text,Text> {
        private final static String white = new String("White");
        private final static String black = new String("Black");
        private final static String draw = new String("draw");

	public void reduce(Text key, Iterable<CustomResults> values, Context context) throws IOException, InterruptedException {
        long black_win = 0;
        long black_draw = 0;
        long black_loss = 0;
        long black_total = 0;
        long white_win = 0;
        long white_draw = 0;
        long white_loss = 0;
        long white_total = 0;

	    for (CustomResults val : values) {
	        if (val.getColor().toString().equals(black)) {
                int temp = val.getGamesWon().get();
                black_win += temp;
                black_total += temp;
                temp = val.getGamesLost().get();
                black_loss += temp;
                black_total += temp;
                temp = val.getGamesDrawn().get();
                black_draw += temp;
                black_total += temp;
            }

	        if (val.getColor().toString().equals(white)) {
                int temp = val.getGamesWon().get();
                white_win += temp;
                white_total += temp;
                temp = val.getGamesLost().get();
                white_loss += temp;
                white_total += temp;
                temp = val.getGamesDrawn().get();
                white_draw += temp;
                white_total += temp;
            }
        }
         
        //All the  black game and white game results for a player has
        //been calculated . Now do the necessary calculations for the 
        //final output
        String white_result = new String();
        String black_result = new String();
        if (white_total == 0) { 
            white_result = white + String.format("  0.00  0.00  0.00"); 
        }
        else {
            white_result = white + String.format("  %1.2f  %1.2f  %1.2f" , (float)white_win/white_total , 
                (float)white_loss/white_total , (float)white_draw/white_total);
        }

        if (black_total == 0) {
            black_result = black + String.format("  0.00  0.00  0.00"); 
        }
        else {
            black_result = black + String.format("  %1.2f  %1.2f  %1.2f" , (float)black_win/black_total , 
                (float)black_loss/black_total , (float)black_draw/black_total);
        }

        context.write(key , new Text(white_result));
        context.write(key , new Text(black_result));
      }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ChessGameB");
        job.setJarByClass(ChessGamePartB.class);
        job.setMapperClass(ChessGameMapper.class);
        job.setCombinerClass(ChessGameCombiner.class);
        job.setReducerClass(ChessGameReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CustomResults.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
