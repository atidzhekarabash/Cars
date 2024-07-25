package Uni.fmi.Cars;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.net.URI;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.RunningJob;

import com.google.common.base.Optional;
import com.google.protobuf.UnknownFieldSet.Field;

public class Car extends JFrame {
	public static final String AVERAGE = "average";
	public static final String LIST = "list";

	private void setGUIProperties() {

		JPanel panel = new JPanel();

		JLabel labelBrand = new JLabel("Brand");
		JLabel labelHpMin = new JLabel("HpMin ");
		JLabel labelHpMax = new JLabel("HpMax");
		JLabel labelMpg = new JLabel("MPG");
		final JComboBox comboBox = new JComboBox();
		final JTextField inputBrand = new JTextField();
		final JTextField inputMPG = new JTextField();
		final JTextField inputHpMin = new JTextField();
		final JTextField inputHpMax = new JTextField();
		JButton search = new JButton("Search");

		panel.setLayout(null);

		
		
		comboBox.setBounds(40, 60, 150, 30);
		labelBrand.setBounds(25, 127, 150, 30);
		inputBrand.setBounds(40, 150, 150, 30);
		search.setBounds(275, 450, 145, 28);
		labelHpMin.setBounds(125, 215, 150, 50);
		inputHpMin.setBounds(130, 250, 150, 30);
		labelHpMax.setBounds(380, 230, 150, 30);
		inputHpMax.setBounds(400, 255, 150, 30);
		labelMpg.setBounds(35, 330, 150, 30);
		inputMPG.setBounds(40, 355, 150, 30);

		
		comboBox.addItem("");
		comboBox.addItem("Average expenses by brand");
		comboBox.addItem("List of cars");

	
        panel.add(labelBrand);
        panel.add(labelHpMin);
        panel.add(labelHpMax);
        panel.add(labelMpg);
		panel.add(comboBox);
		panel.add(inputBrand);
		panel.add(search);
		panel.add(inputHpMin);
		panel.add(inputHpMax);
		panel.add(inputMPG);

		add(panel);
		setBounds(300, 300, 740, 600);
		setVisible(true);

		comboBox.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {

				if (comboBox.getSelectedItem() != null) {
					if (comboBox.getSelectedItem() == "Average expenses by brand") {
						inputMPG.setEnabled(false);
						inputHpMin.setEnabled(false);
						inputHpMax.setEnabled(false);

					} else {
						inputMPG.setEnabled(true);
						inputHpMin.setEnabled(true);
						inputHpMax.setEnabled(true);
					}
				}

			}
		}

		);

		search.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				String text = inputBrand.getText();
				String mpg = inputMPG.getText();
				String hpMin = inputHpMin.getText();
				String hpMax = inputHpMax.getText();
				String selection = "";

				if (comboBox.getSelectedItem() == "Average expense by mark") {
					selection = AVERAGE ;
				} else if (comboBox.getSelectedItem() == "List of cars") {
					selection = LIST;
				}

				startHadoop(text, mpg, hpMin, hpMax, selection);
			}

		});

	}

	protected void startHadoop(String brand, String mpg, String hpMin, String hpMax, String selection) {

		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf, Car.class);
		
		job.set("car", brand);
		job.set("selection", selection);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapperClass(CarMapper.class);
		job.setReducerClass(CarReducer.class);


		Path input = new Path("hdfs://127.0.0.1:9000/input/cars.csv");
		Path output = new Path("hdfs://127.0.0.1:9000/cars");

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		try {
			FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);

			if (fs.exists(output))
				fs.delete(output, true);

			RunningJob task = JobClient.runJob(job);

			System.out.println("Is successfull: " + task.isSuccessful());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {

		Car car = new Car();
		car.setGUIProperties();

	}

}
