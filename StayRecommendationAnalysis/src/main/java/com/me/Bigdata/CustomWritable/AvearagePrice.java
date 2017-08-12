package com.me.Bigdata.CustomWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AvearagePrice implements Writable{

	

		double mean = 0, count = 0,sum =0;

		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			mean = arg0.readDouble();
			count = arg0.readDouble();
			sum = arg0.readDouble();
		}

		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			arg0.writeDouble(mean);
			arg0.writeDouble(count);
			arg0.writeDouble(sum);
		}

		

		

		public double getMean() {
			return mean;
		}

		public void setMean(double mean) {
			this.mean = mean;
		}

		public double getCount() {
			return count;
		}

		public void setCount(double count) {
			this.count = count;
		}

		public double getSum() {
			return sum;
		}

		public void setSum(double sum) {
			this.sum = sum;
		}

		@Override
		public String toString() {
			return "mean" + " :" + mean + " " + "Count" + " :" + count + "Sum " + " :" + sum;
		}
	}

