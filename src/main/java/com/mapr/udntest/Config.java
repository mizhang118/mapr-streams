package com.mapr.udntest;

public class Config {
	protected String topic = null;
	protected double testRate = 1;
	protected String testData = "test.data";

	protected long skipSize = 0;
	protected long count = 0;
	protected boolean done = false;

	public Config() {
	}
	
	public void setTestRate(double testRate) {
		this.testRate = testRate;
	}
	
	public String getTopic() {
		return this.topic;
	}
}
