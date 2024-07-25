package Uni.fmi.Cars;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.google.protobuf.UnknownFieldSet.Field;

public class CarMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	String selection;
	String car;
	Double mpg;
	Double hpMin;
	Double hpMax;

	@Override
	public void configure(JobConf job) {
		selection = job.get("selection", "");
		car = job.get("car", "");
		mpg = job.getDouble("mpg", 0.0);
		hpMin = job.getDouble("hpMin", 0.0);
		hpMax = job.getDouble("hpMax", 0.0);
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {

		String[] columns = value.toString().split(";");

		String fileModel;
		double fileMPG;
		double fileHP;
		double sum;

		try {

			fileModel = columns[0];
			fileMPG = Double.parseDouble(columns[2].toString());
			fileHP = Double.parseDouble(columns[5].toString());
			sum = Double.parseDouble(columns[2]);

		} catch (NumberFormatException ex) {
			return;
		}

		if (selection.equals(Car.AVERAGE )) {
			if (fileModel.toLowerCase().contentEquals(car) && !(car.isEmpty())) {
				try {

					Text outputKey = new Text(fileModel);
					output.collect(outputKey, new DoubleWritable(sum));

					return;
				} catch (NumberFormatException ex) {
					System.err.println(value.toString());
				}
			}
		} else if (selection.equals(Car.LIST)) {
			boolean filterMatch = true;

			if (!car.isEmpty() && filterMatch) {
				filterMatch = fileModel.toLowerCase().contentEquals(car);
			}

			if (hpMin != 0 && hpMax != 0 && filterMatch) {
				filterMatch = fileHP >= hpMin && fileHP <= hpMax;
			}

			if (mpg != 0 && filterMatch) {
				filterMatch = fileMPG > mpg;
			}

			if (filterMatch) {
				try {

					Text outputKey = new Text(fileModel + "--------" + fileHP + " : " + fileMPG);
					output.collect(outputKey, new DoubleWritable());

					return;
				} catch (NumberFormatException ex) {
					System.err.println(value.toString());
				}
			}
		}

	}
}
