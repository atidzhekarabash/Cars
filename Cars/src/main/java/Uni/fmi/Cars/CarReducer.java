package Uni.fmi.Cars;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CarReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@SuppressWarnings("unlikely-arg-type")
	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {

		int count = 0;
		double sum = 0.0;

		while (values.hasNext()) {
			count++;
			sum += values.next().get();
		}

		double avrgMPG = sum / count;
		double roundOff = Math.round(avrgMPG * 100.0) / 100.0;
		output.collect(key, new DoubleWritable(Math.abs(roundOff)));

	}

}
