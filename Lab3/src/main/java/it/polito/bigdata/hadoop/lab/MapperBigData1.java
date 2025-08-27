package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String[] riga=value.toString().split(",");
            String[] prodotti_ID= Arrays.copyOfRange(riga, 1, riga.length);
            int ind = prodotti_ID.length;

            if (ind>1) {
                for (int i =0; i<ind-1; i++) {
                    for (int j = i+1; j<ind; j++) {
                        String prodotto1 = prodotti_ID[i];
                        String prodotto2 = prodotti_ID[j];
                        String pair_prodotti= prodotto1 + "+" + prodotto2;
                        context.write(new Text(pair_prodotti), new IntWritable(1));
                    }
                }
            }

    }
}
