package com.me.Bigdata.CustomWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TopCustomerDetails implements Writable {

	double customerId;
	int numberOfReviews;

	public TopCustomerDetails() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(numberOfReviews);
		out.writeDouble(customerId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		numberOfReviews = in.readInt();
		customerId = in.readDouble();
	}

}
