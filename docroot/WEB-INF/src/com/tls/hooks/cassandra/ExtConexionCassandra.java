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
		 return session;
	}


}
