import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/*User defined class to store the 
 * white results and the black results */
public class CustomResults implements Writable {
    public static Text black =  new Text("Black");
    public static Text white =  new Text("White");

    private IntWritable won = new IntWritable(0);
    private IntWritable loss = new IntWritable(0);
    private IntWritable draw = new IntWritable(0);
    private Text color = new Text();

    public CustomResults() {
    }

    public CustomResults (Text color) {
        this.color = color;
    }

    public CustomResults (CustomResults object) {
        this.color = object.color;
        this.won = object.won;
        this.loss = object.loss;
        this.draw = object.draw;
    }

    public void setGameWon(int val) {
        won.set(val);
    }

    public void setGameLost(int val) {
        loss.set(val);
    }

    public void setGameDrawn(int val) {
        draw.set(val);
    }

    public IntWritable getGamesWon() { 
        return won;
    }

    public IntWritable getGamesLost() { 
        return loss;
    }

    public IntWritable getGamesDrawn() { 
        return draw;
    }

    public Text getColor() {
        return color;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        color.readFields(in);
        won.readFields(in);
        loss.readFields(in);
        draw.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        color.write(out);
        won.write(out);
        loss.write(out);
        draw.write(out);
    }
}   
