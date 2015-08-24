package com.liferay.counter.model;

import com.liferay.portal.kernel.concurrent.CompeteLatch;

/**
 * @author Harry Mark
 * @author Shuyang Zhou
 * @author Edward Han
 */
public class CounterRegister {

	public CounterRegister(
		String name, CounterHolder counterHolder, int rangeSize) {

		_name = name;
		_rangeSize = rangeSize;
		_counterHolder = counterHolder;
		_competeLatch = new CompeteLatch();
	}

	public CounterRegister(
		String name, long rangeMin, long rangeMax, int rangeSize) {

		this(name, new CounterHolder(rangeMin, rangeMax), rangeSize);
	}

	public CompeteLatch getCompeteLatch() {
		return _competeLatch;
	}

	public CounterHolder getCounterHolder() {
		return _counterHolder;
	}

	public String getName() {
		return _name;
	}

	public int getRangeSize() {
		return _rangeSize;
	}

	public void setCounterHolder(CounterHolder holder) {
		_counterHolder = holder;
	}

	public void setName(String name) {
		_name = name;
	}

	private final CompeteLatch _competeLatch;
	private volatile CounterHolder _counterHolder;
	private String _name;
	private final int _rangeSize;

}