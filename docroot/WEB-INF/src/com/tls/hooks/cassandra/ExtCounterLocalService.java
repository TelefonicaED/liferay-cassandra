package com.tls.hooks.cassandra;

import java.util.List;

import com.liferay.counter.NoSuchCounterException;
import com.liferay.counter.model.Counter;
import com.liferay.counter.service.CounterLocalService;
import com.liferay.counter.service.CounterLocalServiceUtil;
import com.liferay.counter.service.CounterLocalServiceWrapper;
import com.liferay.counter.service.persistence.CounterFinderUtil;
import com.liferay.portal.kernel.dao.orm.DynamicQuery;
import com.liferay.portal.kernel.dao.orm.DynamicQueryFactoryUtil;
import com.liferay.portal.kernel.dao.orm.PropertyFactoryUtil;
import com.liferay.portal.kernel.exception.PortalException;
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
import com.datastax.shaded.netty.util.internal.ConcurrentHashMap;





import java.util.Map;



public class ExtCounterLocalService extends CounterLocalServiceWrapper {
	
	Session session = ExtConexionCassandra.getSesion();
	
	PreparedStatement addCounterStatement;
	PreparedStatement deteleCounterStatement;
	PreparedStatement getCounterStatement;
	PreparedStatement getCountersStatement;		
	PreparedStatement getCountersCountStatement;	
	PreparedStatement getNamesStatement;
	
	
	   public void createSchema() {
		      session.execute("CREATE KEYSPACE IF NOT EXISTS liferay WITH replication " + 
		            "= {'class':'SimpleStrategy', 'replication_factor':1};");
		      session.execute(
		            "CREATE TABLE IF NOT EXISTS liferay.counter (" +
		                  "name varchar," +
		            	  "currentId counter, "+	
		                  "PRIMARY KEY (name)" + 
		                  ") ;");
		   }
	   
		private void preparedStatements () {
			
			
			 // SELECT
			
			getCounterStatement = session.prepare(
		  			"select * from liferay.counter where  name=?;");
			
			getCountersStatement = session.prepare(
		  			"select * from liferay.counter LIMIT ?;");
			
			getCountersCountStatement = session.prepare(
		  			"select count(*) from liferay.counter;");
			
			getNamesStatement = session.prepare(
		  			"select name from liferay.counter;");
			
             // INSERT O UDATE
			
			addCounterStatement = session.prepare(
		  			"update  liferay.counter SET currentid = currentid + ? WHERE  name=? ;"); 	
			
			// DELETE
			
			deteleCounterStatement = session.prepare(
		  			"DELETE FROM liferay.counter where  name= ?;");
 	
			
		}
	

	public ExtCounterLocalService(CounterLocalService counterLocalService) {
		super(counterLocalService);
 	    createSchema();
 	    preparedStatements();
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
		System.out.println("addCounter");

		
		BoundStatement boundStatement = new BoundStatement(addCounterStatement);
		session.execute(boundStatement.bind(counter.getCurrentId(), counter.getName()));

		return counter;
	}

	/**
	* Creates a new counter with the primary key. Does not add the counter to the database.
	*
	* @param name the primary key for the new counter
	* @return the new counter
	*/
	public com.liferay.counter.model.Counter createCounter(
		java.lang.String name) {
		System.out.println("createCounter");

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
		System.out.println("deleteCounter1");

		//Search the counter
		Counter counter =this.getCounter(name);
		
		
		BoundStatement boundStatement = new BoundStatement(deteleCounterStatement);
		ResultSet results=session.execute(boundStatement.bind(counter.getName()));

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

		System.out.println("deleteCounter2");

	
			try {
				this.deleteCounter(counter.getName());
			} catch (PortalException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return counter;
		
		//return super.deleteCounter(counter);
	}

	public com.liferay.portal.kernel.dao.orm.DynamicQuery dynamicQuery() {
		System.out.println("dynamicQuery1");
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
		System.out.println("dynamicQuery2");

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
		System.out.println("dynamicQuery3");

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
		System.out.println("dynamicQuery4");

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
		System.out.println("dynamicQueryCount");

		return super.dynamicQueryCount(dynamicQuery);
	}

	/**
	 * Returns the counter with the primary key or returns <code>null</code> if it could not be found.
	 *
	 * @param name the primary key of the counter
	 * @return the counter, or <code>null</code> if a counter with the primary key could not be found
	 * @throws SystemException if a system exception occurred
	 */
	public com.liferay.counter.model.Counter fetchCounter(java.lang.String name)
		throws com.liferay.portal.kernel.exception.SystemException {
		System.out.println("fetchCounter");

		
		try {
			  return this.getCounter(name);
		} catch (PortalException e) {
			// TODO Auto-generated catch block
			return null;
		}
		//return super.fetchCounter(name);
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
		System.out.println("getCounter");

		
		
		BoundStatement boundStatement = new BoundStatement(getCounterStatement);
		ResultSet results=session.execute(boundStatement.bind(name));
		if(results!=null ){
			List<Row> rowlist=results.all();
			if(rowlist.size()>0){
				Row row=rowlist.get(0);
				Counter counter = getCounterFromRow(row);
				return counter;
			}else{
					throw new NoSuchCounterException( "No Counter exists with the primary key " +
						name);
			}
		}
		return null;
		
		//return super.getCounter(name);
	}

	
	/**
	 * Returns the counter with the primary key or throws a {@link com.liferay.portal.NoSuchModelException} if it could not be found.
	 *
	 * @param primaryKey the primary key of the counter
	 * @return the counter
	 * @throws com.liferay.portal.NoSuchModelException if a counter with the primary key could not be found
	 * @throws SystemException if a system exception occurred
	 */	
	public com.liferay.portal.model.PersistedModel getPersistedModel(
		java.io.Serializable primaryKeyObj)
		throws com.liferay.portal.kernel.exception.PortalException,
			com.liferay.portal.kernel.exception.SystemException {
		
		System.out.println("getPersistedModel");

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
		
		System.out.println("getCounters");

		List<Counter> counters=new java.util.ArrayList<Counter>();
		BoundStatement boundStatement = new BoundStatement(getCountersStatement);
		boundStatement.setFetchSize(end);
		ResultSet results=session.execute(boundStatement.bind(end));
		List<Row> rows=results.all();
		if(rows.size()>0&&rows.size()>=start){
			for(int i=start;i<rows.size();i++){
				Row row=rows.get(i);
				Counter counter = getCounterFromRow(row);
				counters.add(counter);
			}
		}
		return counters;			
		
		//return super.getCounters(start, end);
	}

	/**
	* Returns the number of counters.
	*
	* @return the number of counters
	* @throws SystemException if a system exception occurred
	*/
	public int getCountersCount()
		throws com.liferay.portal.kernel.exception.SystemException {
		System.out.println("getCountersCount");

		BoundStatement boundStatement = new BoundStatement(getCountersCountStatement);
		ResultSet results=session.execute(boundStatement.bind());
		List<Row> rows=results.all();
		int cont =0;
		cont = (int) rows.get(0).getLong(0);
		return cont; 
		
		//return super.getCountersCount();
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
		
		System.out.println("updateCounter");

		
		return this.addCounter(counter);
		//return super.updateCounter(counter);
		
		
		
		
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
		System.out.println("updateCounter");

		return super.updateCounter(counter, merge);
	}

	/**
	* Returns the Spring bean ID for this bean.
	*
	* @return the Spring bean ID for this bean
	*/
	public java.lang.String getBeanIdentifier() {
		System.out.println("getBeanIdentifier");

		return super.getBeanIdentifier();
	}

	/**
	* Sets the Spring bean ID for this bean.
	*
	* @param beanIdentifier the Spring bean ID for this bean
	*/
	public void setBeanIdentifier(java.lang.String beanIdentifier) {
		System.out.println("setBeanIdentifier");
		super.setBeanIdentifier(beanIdentifier);
	}

	public java.util.List<java.lang.String> getNames()
		throws com.liferay.portal.kernel.exception.SystemException {
		System.out.println("getNames");

				List<String> counters=new java.util.ArrayList<String>();
				counters =null;
				BoundStatement boundStatement = new BoundStatement(getNamesStatement);
				ResultSet results=session.execute(boundStatement.bind());
				List<Row> rows=results.all();
				if(rows.size()>0&&rows.size()>=0){
					for(int i=0;i<rows.size();i++){
						Row row=rows.get(i);
 						counters.add(row.getString("name"));
					}
				}
				return counters;					
		//return super.getNames();
	}

	public long increment()
		throws com.liferay.portal.kernel.exception.SystemException {
		System.out.println("increment1");

	    
		return increment(_NAME);
		
		
		//return super.increment(name);
		
	}

	public long increment(java.lang.String name)
		throws com.liferay.portal.kernel.exception.SystemException {
		System.out.println("increment2");
		
		try {
			return increment(name,_MINIMUM_INCREMENT_SIZE);
		} catch (PortalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	
	// *****************************************************************************************************
	//
	//Este codigo est� preparado para interrelacionar los contadores de Cassandra con Counter de Liferay.
	//Sino  se usa estas comprobaciones los contadores inicializar�an a 1con lo cual dar�a error Duplicate Key en tablas de Liferay,
	//
	// *****************************************************************************************************
   
	public long increment(java.lang.String name, long size)
		throws com.liferay.portal.kernel.exception.SystemException, PortalException {
		System.out.println("increment3");
		
		
		// To Do Cache
		
		//*********************************//
		
		if (size < _MINIMUM_INCREMENT_SIZE) {
			size = _MINIMUM_INCREMENT_SIZE;
		}
		

		//Comprobacion si existe contador para la clase en Liferay
		
		DynamicQuery query = DynamicQueryFactoryUtil.forClass(Counter.class)
				.add(PropertyFactoryUtil.forName("name").eq(name));

	     List  lista=  CounterLocalServiceUtil.dynamicQuery(query);		
	    
	     Counter counter = (Counter) lista.get(0);

			
	    long contador=0;
	    
	    if (counter == null) { //No existe en Counter
	    	BoundStatement boundStatement = new BoundStatement(addCounterStatement);
			session.execute(boundStatement.bind(size,name));

	    }else{ //Existe en Counter
	    	 //Buscar en cassandra si existe, se incrementa 1
			// Get CurrentId
	    	BoundStatement boundStatement = new BoundStatement(getCounterStatement);
			ResultSet results=session.execute(boundStatement.bind(name));
			List<Row> rows=results.all();

			if (rows.isEmpty()){ // No existe
				// Si existe, se busca el valor y se incrementa 1 
				boundStatement = new BoundStatement(addCounterStatement);
				session.execute(boundStatement.bind(counter.getCurrentId()+1 ,name));
				contador = counter.getCurrentId() +1;
			}else{
					boundStatement = new BoundStatement(addCounterStatement);			
				    results=session.execute(boundStatement.bind(size,name));
				    
				    boundStatement = new BoundStatement(getCounterStatement);
				    results = session.execute(boundStatement.bind(name));
					 rows=results.all();
					 Row row=rows.get(0);
					 
					contador = row.getLong(1);
			}
	    }
	    
		return contador;		

		//return super.increment(name, size);
	}

	public void rename(java.lang.String oldName, java.lang.String newName)
		throws com.liferay.portal.kernel.exception.SystemException {
		System.out.println("rename");

		super.rename(oldName, newName);
	}

	public void reset(java.lang.String name)
		throws com.liferay.portal.kernel.exception.SystemException {
		System.out.println("reset1");

		super.reset(name);
	}

	public void reset(java.lang.String name, long size)
		throws com.liferay.portal.kernel.exception.SystemException {
		System.out.println("reset2");

		super.reset(name, size);
	}

    


	
	private Counter getCounterFromRow(Row row)
			throws SystemException {
		Counter counter= this.createCounter(row.getString("name"));
		counter.setCurrentId(row.getLong("currentId"));
		return counter;
	}

	private static final long _MINIMUM_INCREMENT_SIZE = 1;
	private static final String _NAME = Counter.class.getName();

}
