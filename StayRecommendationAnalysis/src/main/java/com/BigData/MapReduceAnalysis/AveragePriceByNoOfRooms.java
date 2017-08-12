package com.BigData.MapReduceAnalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.BigData.utils.ColumnParser;
import com.BigData.utils.HBaseTablesName;

public class AveragePriceByNoOfRooms {
	public static void main(String[] args) throws Exception {

		if (args.length <= 0 || args.length % 2 != 0) {
			System.out.println("Please sepcify the inputarguments in this format:");
			System.out.println(
					"hadoop jar <jar location> <path to main class> <city name> <path to input file, listings.csv>, ...");
			System.exit(0);
		}
		List<String> cities = new ArrayList<>();
		List<String> pathToInputFIles = new ArrayList<>();
		for (int i = 0; i < args.length; i++) {
			if (i % 2 == 0) {
				cities.add(args[i]);
			} else {
				pathToInputFIles.add(args[i]);
			}
		}
		for (int i = 0; i < cities.size(); i++) {
			Configuration conf = HBaseConfiguration.create();
			conf.set("Place", cities.get(i));
			Job job = Job.getInstance(conf, "AvaerageAnalysisByPriceByNoOfRooms");
			job.setJarByClass(AveragePriceByNoOfRooms.class);
			job.setMapperClass(AveragePriceByNoOfRoomsMapper.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(SortedMapWritable.class);
			job.addCacheFile(new URI("/Stay/Cache/headerForBerlin#headerForBerlin"));
			FileInputFormat.addInputPath(job, new Path(pathToInputFIles.get(i)));
			TableMapReduceUtil.initTableReducerJob(HBaseTablesName.tableNameForAnalysisOfListingByPlace,
					AveragePriceByNoOfRoomReducer.class, job);
			job.waitForCompletion(true);
		}
		System.exit(0);
	}

	private static class AveragePriceByNoOfRoomsMapper
			extends Mapper<LongWritable, Text, IntWritable, SortedMapWritable> {

		private IntWritable outKey = new IntWritable();
		private LongWritable one = new LongWritable(1);
		private int indexOfBedRooms = 0;
		private int indexOfprice = 0;

		String[] headerList;
		static {
			File f = new File("/Users/akashnagesh/Desktop/mapredSysout");
			try {
				System.setOut(new PrintStream(f));
			} catch (Exception e) {

			}
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, IntWritable, SortedMapWritable>.Context context)
				throws IOException, InterruptedException {
			BufferedReader bufferedReader = new BufferedReader(new FileReader("headerForBerlin"));
			try {

				headerList = bufferedReader.readLine().split("\t");
				indexOfprice = ColumnParser.getTheIndexOfTheColumn(headerList, "price");
				indexOfBedRooms = ColumnParser.getTheIndexOfTheColumn(headerList, "bedrooms");

			} catch (Exception e) {
				System.out.println("Something Went Wrong");
			} finally {
				bufferedReader.close();
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String val[] = value.toString().split("\t");
			if (value.toString().contains("price") || val.length > 95)
				return;

			SortedMapWritable outValue = new SortedMapWritable();
			DoubleWritable doubleWritable;
			try {
				doubleWritable = new DoubleWritable(Double.valueOf(val[indexOfprice].replace('$', ' ').trim()));
			} catch (Exception e) {
				return;
			}
			outValue.put(doubleWritable, one);
			try {
				// System.out.println(val[indexOfBedRooms]);
				outKey.set(Integer.parseInt(val[indexOfBedRooms]));
			} catch (Exception e) {
				System.out.println("Not an number");
				return;
			}

			context.write(outKey, outValue);
		}

	}

	private static class AveragePriceByNoOfRoomReducer
			extends TableReducer<IntWritable, SortedMapWritable, ImmutableBytesWritable> {

		static {
			File f = new File("/Users/akashnagesh/Desktop/mapredSysout");
			try {
				System.setOut(new PrintStream(f));
			} catch (Exception e) {

			}
		}
		final List<Double> priceList = new ArrayList<Double>();

		Connection connection;
		Admin admin;

		Table table;

		@Override
		protected void setup(Reducer<IntWritable, SortedMapWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {

			connection = ConnectionFactory.createConnection(context.getConfiguration());
			// Get Admin
			admin = connection.getAdmin();

			if (admin.tableExists(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace))) {
				// Use the created table if its already exists to check the
				// column family
				table = connection.getTable(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace));

				if (!table.getTableDescriptor().hasFamily(Bytes.toBytes("AveragePriceByNoOfRooms"))) {

					// Create the Column family
					HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("AveragePriceByNoOfRooms");

					// Add the column family
					admin.addColumn(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace),
							hColumnDescriptor);

				}
			} else {

				// Create an Table Descriptor with table name
				HTableDescriptor tablefoThisJob = new HTableDescriptor(
						TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace));

				// create an column descriptor for the table
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("AveragePriceByNoOfRooms");

				// add the column family for the table
				tablefoThisJob.addFamily(hColumnDescriptor);

				// This will create an new Table in the HBase
				admin.createTable(tablefoThisJob);
			}
		}

		@Override
		protected void reduce(IntWritable key, Iterable<SortedMapWritable> values,
				Reducer<IntWritable, SortedMapWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {

			priceList.clear();
			double sum = 0;
			double count = 0;
			double averagePriceNoOfRoomsType = 0;

			for (SortedMapWritable sm : values) {
				for (Map.Entry<WritableComparable, Writable> entry : sm.entrySet()) {
					long numberOfTimes = ((LongWritable) entry.getValue()).get();

					for (long i = 0; i < numberOfTimes; i++) {
						double val = ((DoubleWritable) entry.getKey()).get();
						priceList.add(val);
						sum += val;
					}
				}
				sm.clear();
			}

			count = priceList.size();
			averagePriceNoOfRoomsType = sum / count;

			Put putTolistingsAnalyisByPlace = new Put(Bytes.toBytes(context.getConfiguration().get("Place")));

			String noOfRoom = "";
			System.out.println("IN Reducer" + key.toString());
			System.out.println(key.get());
			// Adding Average price for No of romms into hBase table
			if (key.get() == 1) {
				System.out.println("one");
				noOfRoom = "one";
			} else if (key.get() == 2) {
				System.out.println("two");
				noOfRoom = "two";
			} else if (key.get() == 3) {
				System.out.println("three");
				noOfRoom = "three";
			} else {
				System.out.println("fourPlus");
				noOfRoom = "fourPlus";
			}
			// System.out.println(averagePriceNoOfRoomsType);
			putTolistingsAnalyisByPlace.addColumn(Bytes.toBytes("AveragePriceByNoOfRooms"),
					Bytes.toBytes(noOfRoom.toString()), Bytes.toBytes(averagePriceNoOfRoomsType));

			// Will Write to ListingsAnalyisByPlace HBase Table
			context.write(null, putTolistingsAnalyisByPlace);

		}

		@Override
		protected void cleanup(
				Reducer<IntWritable, SortedMapWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (connection != null)
				// Close the Connection
				connection.close();
			if (table != null)
				// Close the table Connection
				table.close();
		}

	}

}
