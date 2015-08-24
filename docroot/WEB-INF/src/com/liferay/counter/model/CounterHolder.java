package com.liferay.counter.model;

import java.util.concurrent.atomic.AtomicLong;

public class CounterHolder {
	
	public CounterHolder(long initValue, long rangeMax) {
		_counter = new AtomicLong(initValue);
		_rangeMax = rangeMax;
	}

	public long addAndGet(long delta) {
		return _counter.addAndGet(delta);
	}

	public long getCurrentValue() {
		return _counter.get();
	}

	public long getRangeMax() {
		return _rangeMax;
	}

	private final AtomicLong _counter;
	private final long _rangeMax;
	

}
