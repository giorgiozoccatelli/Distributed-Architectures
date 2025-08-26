package it.polito.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import it.polito.bigdata.hadoop.DriverBigData.COUNTERS;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    String prefix;

    protected void setup(Context context) throws IOException, InterruptedException {
    		/* Get the prefix from the configuration */
        prefix=context.getConfiguration().get("prefix").toString();
    }
    
    protected void map(
        Text key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {
            
        if () {
            context.write(key, value);
            context.getCounter(COUNTERS.SELECTED_WORDS).increment(1);
        }
        else {
            context.getCounter(COUNTERS.DISCARDED_WORDS).increment(1);
        }
    }
}
