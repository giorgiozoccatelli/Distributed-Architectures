package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    IntWritable,         // Input value type
                    NullWritable,         // Output key type
                    WordCountWritable> {// Output value type

    private TopKVector<WordCountWritable> localTopK;
    private Integer k = 100;

    protected void setup(Context context) throws IOException, InterruptedException {
        localTopK = new TopKVector<WordCountWritable>(k);
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
            
            WordCountWritable currentPair = new WordCountWritable(key.toString(), Integer.valueOf((Integer.parseInt(value.toString())))); //Text->String, String->int, int->Integer
    		localTopK.updateWithNewElement(currentPair);

    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (WordCountWritable p : localTopK.getLocalTopK()) {
            context.write(NullWritable.get(), new WordCountWritable(p));
        }
    }
}

