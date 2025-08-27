package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                WordCountWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    private Integer k =100;
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<WordCountWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        TopKVector<WordCountWritable> globTopK = new TopKVector<WordCountWritable>(k);

        for (WordCountWritable currentPair: values ){
            globTopK.updateWithNewElement(new WordCountWritable(currentPair));
        }
    	
        for (WordCountWritable p:globTopK.getLocalTopK()){
            context.write(new Text(p.getWord()), new IntWritable(p.getCount()));
        }

    }
}
