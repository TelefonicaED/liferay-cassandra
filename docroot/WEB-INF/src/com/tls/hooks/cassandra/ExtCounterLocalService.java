package com.tls.hooks.cassandra;

import java.util.List;

import com.liferay.counter.service.CounterLocalService;
import com.liferay.counter.service.CounterLocalServiceWrapper;
import com.liferay.portal.kernel.exception.SystemException;
import com.liferay.portal.kernel.transaction.Isolation;
import com.liferay.portal.kernel.transaction.Propagation;
import com.liferay.portal.kernel.transaction.Transactional;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;



public class ExtCounterLocalService extends CounterLocalServiceWrapper {

	public ExtCounterLocalService(CounterLocalService counterLocalService) {
		super(counterLocalService);
		// TODO Auto-generated constructor stub
	}



	/**
	* Adds the counter to the database. Also notifies the appropriate model listeners.
	*
	* @param counter the counter
	* @return the counter that was added
	* @throws SystemException if a system exception occurred
	*/
	public com.liferay.counter.model.Counter addCounter(
		com.liferay.counter.model.Counter counter)
		throws com.liferay.portal.kernel.exception.SystemException {

		return super.addCounter(counter);
	}

	/**
	* Creates a new counter with the primary key. Does not add the counter to the database.
	*
	* @param name the primary key for the new counter
	* @return the new counter
	*/
	public com.liferay.counter.model.Counter createCounter(
		java.lang.String name) {
		return super.createCounter(name);
	}

	/**
	* Deletes the counter with the primary key from the database. Also notifies the appropriate model listeners.
	*
	* @param name the primary key of the counter
	* @return the counter that was removed
	* @throws PortalException if a counter with the primary key could not be found
	* @throws SystemException if a system exception occurred
	*/
	public com.liferay.counter.model.Counter deleteCounter(
		java.lang.String name)
		throws com.liferay.portal.kernel.exception.PortalException,
			com.liferay.portal.kernel.exception.SystemException {
		return super.deleteCounter(name);
	}

	/**
	* Deletes the counter from the database. Also notifies the appropriate model listeners.
	*
	* @param counter the counter
	* @return the counter that was removed
	* @throws SystemException if a system exception occurred
	*/
	public com.liferay.counter.model.Counter deleteCounter(
		com.liferay.counter.model.Counter counter)
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.deleteCounter(counter);
	}

	public com.liferay.portal.kernel.dao.orm.DynamicQuery dynamicQuery() {
		return super.dynamicQuery();
	}

	/**
	* Performs a dynamic query on the database and returns the matching rows.
	*
	* @param dynamicQuery the dynamic query
	* @return the matching rows
	* @throws SystemException if a system exception occurred
	*/
	@SuppressWarnings("rawtypes")
	public java.util.List dynamicQuery(
		com.liferay.portal.kernel.dao.orm.DynamicQuery dynamicQuery)
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.dynamicQuery(dynamicQuery);
	}

	/**
	* Performs a dynamic query on the database and returns a range of the matching rows.
	*
	* <p>
	* Useful when paginating results. Returns a maximum of <code>end - start</code> instances. <code>start</code> and <code>end</code> are not primary keys, they are indexes in the result set. Thus, <code>0</code> refers to the first result in the set. Setting both <code>start</code> and <code>end</code> to {@link com.liferay.portal.kernel.dao.orm.QueryUtil#ALL_POS} will return the full result set.
	* </p>
	*
	* @param dynamicQuery the dynamic query
	* @param start the lower bound of the range of model instances
	* @param end the upper bound of the range of model instances (not inclusive)
	* @return the range of matching rows
	* @throws SystemException if a system exception occurred
	*/
	@SuppressWarnings("rawtypes")
	public java.util.List dynamicQuery(
		com.liferay.portal.kernel.dao.orm.DynamicQuery dynamicQuery, int start,
		int end) throws com.liferay.portal.kernel.exception.SystemException {
		return super.dynamicQuery(dynamicQuery, start, end);
	}

	/**
	* Performs a dynamic query on the database and returns an ordered range of the matching rows.
	*
	* <p>
	* Useful when paginating results. Returns a maximum of <code>end - start</code> instances. <code>start</code> and <code>end</code> are not primary keys, they are indexes in the result set. Thus, <code>0</code> refers to the first result in the set. Setting both <code>start</code> and <code>end</code> to {@link com.liferay.portal.kernel.dao.orm.QueryUtil#ALL_POS} will return the full result set.
	* </p>
	*
	* @param dynamicQuery the dynamic query
	* @param start the lower bound of the range of model instances
	* @param end the upper bound of the range of model instances (not inclusive)
	* @param orderByComparator the comparator to order the results by (optionally <code>null</code>)
	* @return the ordered range of matching rows
	* @throws SystemException if a system exception occurred
	*/
	@SuppressWarnings("rawtypes")
	public java.util.List dynamicQuery(
		com.liferay.portal.kernel.dao.orm.DynamicQuery dynamicQuery, int start,
		int end,
		com.liferay.portal.kernel.util.OrderByComparator orderByComparator)
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.dynamicQuery(dynamicQuery, start, end,
			orderByComparator);
	}

	/**
	* Returns the number of rows that match the dynamic query.
	*
	* @param dynamicQuery the dynamic query
	* @return the number of rows that match the dynamic query
	* @throws SystemException if a system exception occurred
	*/
	public long dynamicQueryCount(
		com.liferay.portal.kernel.dao.orm.DynamicQuery dynamicQuery)
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.dynamicQueryCount(dynamicQuery);
	}

	public com.liferay.counter.model.Counter fetchCounter(java.lang.String name)
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.fetchCounter(name);
	}

	/**
	* Returns the counter with the primary key.
	*
	* @param name the primary key of the counter
	* @return the counter
	* @throws PortalException if a counter with the primary key could not be found
	* @throws SystemException if a system exception occurred
	*/
	public com.liferay.counter.model.Counter getCounter(java.lang.String name)
		throws com.liferay.portal.kernel.exception.PortalException,
			com.liferay.portal.kernel.exception.SystemException {
		return super.getCounter(name);
	}

	public com.liferay.portal.model.PersistedModel getPersistedModel(
		java.io.Serializable primaryKeyObj)
		throws com.liferay.portal.kernel.exception.PortalException,
			com.liferay.portal.kernel.exception.SystemException {
		return super.getPersistedModel(primaryKeyObj);
	}

	/**
	* Returns a range of all the counters.
	*
	* <p>
	* Useful when paginating results. Returns a maximum of <code>end - start</code> instances. <code>start</code> and <code>end</code> are not primary keys, they are indexes in the result set. Thus, <code>0</code> refers to the first result in the set. Setting both <code>start</code> and <code>end</code> to {@link com.liferay.portal.kernel.dao.orm.QueryUtil#ALL_POS} will return the full result set.
	* </p>
	*
	* @param start the lower bound of the range of counters
	* @param end the upper bound of the range of counters (not inclusive)
	* @return the range of counters
	* @throws SystemException if a system exception occurred
	*/
	public java.util.List<com.liferay.counter.model.Counter> getCounters(
		int start, int end)
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.getCounters(start, end);
	}

	/**
	* Returns the number of counters.
	*
	* @return the number of counters
	* @throws SystemException if a system exception occurred
	*/
	public int getCountersCount()
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.getCountersCount();
	}

	/**
	* Updates the counter in the database or adds it if it does not yet exist. Also notifies the appropriate model listeners.
	*
	* @param counter the counter
	* @return the counter that was updated
	* @throws SystemException if a system exception occurred
	*/
	public com.liferay.counter.model.Counter updateCounter(
		com.liferay.counter.model.Counter counter)
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.updateCounter(counter);
	}

	/**
	* Updates the counter in the database or adds it if it does not yet exist. Also notifies the appropriate model listeners.
	*
	* @param counter the counter
	* @param merge whether to merge the counter with the current session. See {@link com.liferay.portal.service.persistence.BatchSession#update(com.liferay.portal.kernel.dao.orm.Session, com.liferay.portal.model.BaseModel, boolean)} for an explanation.
	* @return the counter that was updated
	* @throws SystemException if a system exception occurred
	*/
	public com.liferay.counter.model.Counter updateCounter(
		com.liferay.counter.model.Counter counter, boolean merge)
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.updateCounter(counter, merge);
	}

	/**
	* Returns the Spring bean ID for this bean.
	*
	* @return the Spring bean ID for this bean
	*/
	public java.lang.String getBeanIdentifier() {
		return super.getBeanIdentifier();
	}

	/**
	* Sets the Spring bean ID for this bean.
	*
	* @param beanIdentifier the Spring bean ID for this bean
	*/
	public void setBeanIdentifier(java.lang.String beanIdentifier) {
		super.setBeanIdentifier(beanIdentifier);
	}

	public java.util.List<java.lang.String> getNames()
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.getNames();
	}

	public long increment()
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.increment();
	}

	public long increment(java.lang.String name)
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.increment(name);
	}

	public long increment(java.lang.String name, int size)
		throws com.liferay.portal.kernel.exception.SystemException {
		return super.increment(name, size);
	}

	public void rename(java.lang.String oldName, java.lang.String newName)
		throws com.liferay.portal.kernel.exception.SystemException {
		super.rename(oldName, newName);
	}

	public void reset(java.lang.String name)
		throws com.liferay.portal.kernel.exception.SystemException {
		super.reset(name);
	}

	public void reset(java.lang.String name, long size)
		throws com.liferay.portal.kernel.exception.SystemException {
		super.reset(name, size);
	}








}
