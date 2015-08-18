package com.tls.hooks.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class ExtConexionCassandra {

	private static Cluster cluster;
	public static  Session session;
	static String node="127.0.0.1";
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		


	}
	
	
	   public static void createSchema() {
		   

		      session.execute("CREATE KEYSPACE IF NOT EXISTS liferay WITH replication " + 
			            "= {'class':'SimpleStrategy', 'replication_factor':1};");
			      session.execute(
			            "CREATE TABLE IF NOT EXISTS liferay.socialrelation (" +
			                  "relationId bigint," + 
			                  "companyId bigint," +
			                  "createDate timestamp," +		                  
			                  "userId1 bigint," + 
			                  "userId2 bigint," +		                  
			                  "type_ int," +
			                  "PRIMARY KEY (relationId)" + 
			                  ");");


			      session.execute(
				            "CREATE INDEX IF NOT EXISTS sagi7 on liferay.socialrelation (userId1);"
			    		  );
			      session.execute(
				            "CREATE INDEX IF NOT EXISTS sagi8 on liferay.socialrelation (userId2);"
			    		  );

		   
		   
		   
		      session.execute("CREATE KEYSPACE IF NOT EXISTS liferay WITH replication " + 
		            "= {'class':'SimpleStrategy', 'replication_factor':1};");
		      session.execute(
		            "CREATE TABLE IF NOT EXISTS liferay.socialactivity (" +
		                  "activityId bigint," + 
		                  "groupId bigint," + 
		                  "companyId bigint," + 
		                  "userId bigint," + 
		                  "createDate timestamp," +
		                  "mirrorActivityId bigint," +
		                  "classNameId bigint," +
		                  "classPK bigint," +
		                  "type_ int," +
		                  "extraData varchar," +
		                  "receiverUserId bigint," +
		                  "PRIMARY KEY (activityId,createDate)" + 
		                  ") WITH CLUSTERING ORDER BY (createDate DESC);");

		      session.execute(
			            "CREATE INDEX IF NOT EXISTS sagi1 on liferay.socialactivity (classnameid);"
		      );
		      session.execute(
			            "CREATE INDEX IF NOT EXISTS sagi2 on liferay.socialactivity (classpk);"
		      );
		      session.execute(
			            "CREATE INDEX IF NOT EXISTS sagi3 on liferay.socialactivity (groupid);"
		      );
		      session.execute(
			            "CREATE INDEX IF NOT EXISTS sagi4 on liferay.socialactivity (mirrorActivityId);"
		      );
		      session.execute(
			            "CREATE INDEX IF NOT EXISTS sagi5 on liferay.socialactivity (receiverUserId);"
		      );
		      session.execute(
			            "CREATE INDEX IF NOT EXISTS sagi6 on liferay.socialactivity (userId);"
		      );
		      
		      
		      session.execute("CREATE KEYSPACE IF NOT EXISTS liferay WITH replication " + 	
			            "= {'class':'SimpleStrategy', 'replication_factor':1};");
			      session.execute(
			            "CREATE TABLE IF NOT EXISTS liferay.socialactivitycounter (" +
			                  "activityCounterId bigint," + 
			                  "groupId bigint," + 
			                  "companyId bigint," + 
			                  "classNameId bigint," +	
			                  "classPK bigint," +
			                  "name varchar," +
			                  "ownerType int," +
			                  "graceValue int," +		                  
			                  "startPeriod int," +
			                  
			                  "PRIMARY KEY (activityCounterId)" + 
			                  ");");
			      
			      session.execute(
				            "CREATE TABLE IF NOT EXISTS liferay.socialactivitycounter_counter (" +
				                  "activityCounterId bigint," + 
				                  "currentValue counter," +
				                  "totalValue counter," +
				                  "endPeriod counter," +			                  
				                  "PRIMARY KEY (activityCounterId)" + 
				                  ");");		      
			      
			      session.execute(
				            "CREATE INDEX IF NOT EXISTS sagi9 on liferay.socialactivitycounter (groupId);"
			      );
			      session.execute(
				            "CREATE INDEX IF NOT EXISTS sagi10 on liferay.socialactivitycounter (classNameId);"
			      );
			      session.execute(
				            "CREATE INDEX IF NOT EXISTS sagi11 on liferay.socialactivitycounter (classPK);"
			      );
			      session.execute(
				            "CREATE INDEX IF NOT EXISTS sagi12 on liferay.socialactivitycounter (name);"
			      );
			      session.execute(
				            "CREATE INDEX IF NOT EXISTS sagi13 on liferay.socialactivitycounter (ownerType);"
			      );
		      
		      
			      session.execute("CREATE KEYSPACE IF NOT EXISTS liferay WITH replication " + 
				            "= {'class':'SimpleStrategy', 'replication_factor':1};");
				      session.execute(
				            "CREATE TABLE IF NOT EXISTS liferay.counter (" +
				                  "name varchar," +
				            	  "currentId counter, "+	
				                  "PRIMARY KEY (name)" + 
				                  ") ;");

				      
				      
		      
		   }	
	

	   public static void connect() 
	   {
		  
		  if(cluster==null)
		  {
		      cluster = Cluster.builder()
		            .addContactPoint(node)
		            .build();
		      Metadata metadata = cluster.getMetadata();
		      System.out.printf("Connected to cluster: %s\n", 
		            metadata.getClusterName());
		      for ( Host host : metadata.getAllHosts() ) {
		         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
		               host.getDatacenter(), host.getAddress(), host.getRack());
		      }
		      
		      session = cluster.connect();
		      
		      
		      
		      
		      
		  }
		   
	  }

	public static Session getSesion() {
		// TODO Auto-generated method stub
		connect();
		createSchema();
		 return session;
	}


}
