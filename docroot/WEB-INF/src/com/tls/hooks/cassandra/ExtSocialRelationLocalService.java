package com.tls.hooks.cassandra;

import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.liferay.portal.SystemException;
import com.liferay.portal.model.User;
import com.liferay.portal.service.UserLocalServiceUtil;
import com.liferay.portlet.social.RelationUserIdException;
import com.liferay.portlet.social.model.SocialRelation;
import com.liferay.portlet.social.service.SocialRelationLocalService;
import com.liferay.portlet.social.service.SocialRelationLocalServiceWrapper;

public class ExtSocialRelationLocalService extends	SocialRelationLocalServiceWrapper {
	
	private static Cluster cluster;
	private  Session session;
	static String node="127.0.0.1";
	
	
	PreparedStatement getRelationStatement;
	PreparedStatement getRelationUserId1UserId2Type;
	
	
	   public Session getSession() 
	   {
	      return this.session;
	   }
	   public void createSchema() {
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
			            "CREATE INDEX IF NOT EXISTS sagi6 on liferay.socialactivity (userId1);"
		    		  );
		      session.execute(
			            "CREATE INDEX IF NOT EXISTS sagi7 on liferay.socialactivity (userId12);"
		    		  );
		      
	   } 
			   public void connect() 
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
				      createSchema();
				      
				      
				      

				     
				     // INSERT  
				  	 insertStatement = session.prepare(
						      "INSERT INTO liferay.socialactivity " +
						      "(activityid, groupid, companyid, userid, createdate,mirrorActivityid," +
						      "classnameid,classpk,type_,extradata,receiveruserid) " +
						      "VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?);");
				  	 
				  	 // SELECT
					 getRelationStatement = session.prepare(
							   "select * from liferay.socialactivity where relationId= ?;");
					 
					 getRelationUserId1UserId2Type = session.prepare(
							   "select * from liferay.socialactivity where userId1= ? and userId2 = ? and type_ = ? ALLOW FILTERING;");
					 
					 
					 
					 
				  	 getGroupActivitiesStatement = session.prepare(
						      "select * from liferay.socialrelation where groupid= ? limit ?;");
					 getActivityStatement = session.prepare(
							   "select * from liferay.socialactivity where activityid= ?;");
					 getActivitiesStatement = session.prepare(
						      "select * from liferay.socialactivity where userId= ?;");
					 getActivitiesLimitStatement = session.prepare(
						      "select * from liferay.socialactivity where userId= ? limit ?;");
					 getOrganizationUsersActivitiesStatement = session.prepare(
						      "select * from liferay.socialactivity where userId= ? and mirrorActivityId =0   limit ?  ALLOW FILTERING ;");
					 getOrganizationUsersActivitiesCountStatement = session.prepare(
						      "select count(*) from liferay.socialactivity where userId= ? and mirrorActivityId = 0 ALLOW FILTERING ;");
					 getActivitiesCountStatement = session.prepare(
						      "select count(*) from liferay.socialactivity where userId= ?;");			 
					 getActivitiesClassNameClasspkStatament = session.prepare(
						      "select * from liferay.socialactivity where classNameId= ? and classPK = ? ALLOW FILTERING;");
					 getActivitiesreceiverUserIdStatement = session.prepare(
						      "select * from liferay.socialactivity where receiverUserId = ?;");
					 getActivitiesClassNameIdStatement = session.prepare(
						      "select * from liferay.socialactivity where  classNameId= ? limit ?;");	
					 getActivitiesClassNameIdCountStatement = session.prepare(
						      "select count(*) from liferay.socialactivity where  classNameId = ?;");
					 
					 getGroupUsersActivitiesStatement = session.prepare(
						      "select * from liferay.socialactivity where userId= ? and mirrorActivityId=0 limit ?  ALLOW FILTERING;");
					 getGroupUsersActivitiesCountStatement = session.prepare(
						      "select count(*) from liferay.socialactivity where userId= ? and mirrorActivityId=0   ALLOW FILTERING;");
					 
					 getActivitiesMirrorActivityIdClassNameIdClasspkStatament = session.prepare(
						      "select * from liferay.socialactivity where mirrorActivityId= ? and classNameId = ?  and classPK = ?  ALLOW FILTERING;");
					 getActivitiesMirrorActivityIdClassNameIdClasspkCountStatament  = session.prepare(
						      "select count(*) from liferay.socialactivity where mirrorActivityId= ? and classNameId = ?  and classPK = ?  ALLOW FILTERING;");
					 
					 getMirrorActivityStatement = session.prepare(
						      "select * from liferay.socialactivity where mirrorActivityId= ?;");
					 
					 getOrganizationActivitiesStatement = session.prepare(
						      "select * from liferay.socialactivity where groupid= ? and mirrorActivityId=0 limit ?  ALLOW FILTERING;");
					 getOrganizationActivitiesCountStatement = session.prepare(
						      "select count(*) from liferay.socialactivity where groupid= ? and mirrorActivityId=0    ALLOW FILTERING;");
					  
					 //DELETE
					 deleteActivityStatement = session.prepare(
						      "delete  from liferay.socialactivity where ActivityId= ?;");

				      
				  }
			      

				   
			  }
		      
	   }   		  
		      
	

	public ExtSocialRelationLocalService(
			SocialRelationLocalService socialRelationLocalService) {
		super(socialRelationLocalService);
		// TODO Auto-generated constructor stub
	}
	


	 	/**
	 	* Adds the social relation to the database. Also notifies the appropriate model listeners.
	 	*
	 	* @param socialRelation the social relation
	 	* @return the social relation that was added
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public com.liferay.portlet.social.model.SocialRelation addSocialRelation(
	 		com.liferay.portlet.social.model.SocialRelation socialRelation)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.addSocialRelation(socialRelation);
	 	}

	 	/**
	 	* Creates a new social relation with the primary key. Does not add the social relation to the database.
	 	*
	 	* @param relationId the primary key for the new social relation
	 	* @return the new social relation
	 	*/
	 	public com.liferay.portlet.social.model.SocialRelation createSocialRelation(
	 		long relationId) {
	 		return super.createSocialRelation(relationId);
	 	}

	 	/**
	 	* Deletes the social relation with the primary key from the database. Also notifies the appropriate model listeners.
	 	*
	 	* @param relationId the primary key of the social relation
	 	* @return the social relation that was removed
	 	* @throws PortalException if a social relation with the primary key could not be found
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public com.liferay.portlet.social.model.SocialRelation deleteSocialRelation(
	 		long relationId)
	 		throws com.liferay.portal.kernel.exception.PortalException,
	 			com.liferay.portal.kernel.exception.SystemException {
	 		return super.deleteSocialRelation(relationId);
	 	}

	 	/**
	 	* Deletes the social relation from the database. Also notifies the appropriate model listeners.
	 	*
	 	* @param socialRelation the social relation
	 	* @return the social relation that was removed
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public com.liferay.portlet.social.model.SocialRelation deleteSocialRelation(
	 		com.liferay.portlet.social.model.SocialRelation socialRelation)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.deleteSocialRelation(socialRelation);
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
	 		return super.dynamicQuery(dynamicQuery, start,
	 			end, orderByComparator);
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

	 	public com.liferay.portlet.social.model.SocialRelation fetchSocialRelation(
	 		long relationId)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.fetchSocialRelation(relationId);
	 	}

	 	/**
	 	* Returns the social relation with the primary key.
	 	*
	 	* @param relationId the primary key of the social relation
	 	* @return the social relation
	 	* @throws PortalException if a social relation with the primary key could not be found
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public com.liferay.portlet.social.model.SocialRelation getSocialRelation(
	 		long relationId)
	 		throws com.liferay.portal.kernel.exception.PortalException,
	 			com.liferay.portal.kernel.exception.SystemException {
	 		return super.getSocialRelation(relationId);
	 	}

	 	public com.liferay.portal.model.PersistedModel getPersistedModel(
	 		java.io.Serializable primaryKeyObj)
	 		throws com.liferay.portal.kernel.exception.PortalException,
	 			com.liferay.portal.kernel.exception.SystemException {
	 		return super.getPersistedModel(primaryKeyObj);
	 	}

	 	/**
	 	* Returns a range of all the social relations.
	 	*
	 	* <p>
	 	* Useful when paginating results. Returns a maximum of <code>end - start</code> instances. <code>start</code> and <code>end</code> are not primary keys, they are indexes in the result set. Thus, <code>0</code> refers to the first result in the set. Setting both <code>start</code> and <code>end</code> to {@link com.liferay.portal.kernel.dao.orm.QueryUtil#ALL_POS} will return the full result set.
	 	* </p>
	 	*
	 	* @param start the lower bound of the range of social relations
	 	* @param end the upper bound of the range of social relations (not inclusive)
	 	* @return the range of social relations
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public java.util.List<com.liferay.portlet.social.model.SocialRelation> getSocialRelations(
	 		int start, int end)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.getSocialRelations(start, end);
	 	}

	 	/**
	 	* Returns the number of social relations.
	 	*
	 	* @return the number of social relations
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public int getSocialRelationsCount()
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.getSocialRelationsCount();
	 	}

	 	/**
	 	* Updates the social relation in the database or adds it if it does not yet exist. Also notifies the appropriate model listeners.
	 	*
	 	* @param socialRelation the social relation
	 	* @return the social relation that was updated
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public com.liferay.portlet.social.model.SocialRelation updateSocialRelation(
	 		com.liferay.portlet.social.model.SocialRelation socialRelation)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.updateSocialRelation(socialRelation);
	 	}

	 	/**
	 	* Updates the social relation in the database or adds it if it does not yet exist. Also notifies the appropriate model listeners.
	 	*
	 	* @param socialRelation the social relation
	 	* @param merge whether to merge the social relation with the current session. See {@link com.liferay.portal.service.persistence.BatchSession#update(com.liferay.portal.kernel.dao.orm.Session, com.liferay.portal.model.BaseModel, boolean)} for an explanation.
	 	* @return the social relation that was updated
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public com.liferay.portlet.social.model.SocialRelation updateSocialRelation(
	 		com.liferay.portlet.social.model.SocialRelation socialRelation,
	 		boolean merge)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.updateSocialRelation(socialRelation,
	 			merge);
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

	 	/**
	 	* Adds a social relation between the two users to the database.
	 	*
	 	* @param userId1 the user that is the subject of the relation
	 	* @param userId2 the user at the other end of the relation
	 	* @param type the type of the relation
	 	* @return the social relation
	 	* @throws PortalException if the users could not be found, if the users
	 	were not from the same company, or if either of the users was the
	 	default user
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public com.liferay.portlet.social.model.SocialRelation addRelation(
	 		long userId1, long userId2, int type)
	 		throws com.liferay.portal.kernel.exception.PortalException,
	 			com.liferay.portal.kernel.exception.SystemException {
	 		
			if (userId1 == userId2) {
				throw new RelationUserIdException();
			}
            
			User user1 = UserLocalServiceUtil.getUser(userId1);
			User user2 = UserLocalServiceUtil.getUser(userId2);

			if (user1.getCompanyId() != user2.getCompanyId()) {
				throw new RelationUserIdException();
			}

			
			//Search Relation UserId1 and UserId2
			SocialRelation socialrelation =null;
			BoundStatement boundStatement = new BoundStatement(getRelationUserId1UserId2Type);
			ResultSet results=session.execute(boundStatement.bind(userId1,userId2,type));
			if(results!=null )		
			{
				List<Row> rowlist=results.all();
				System.out.println(rowlist.size());
				if(rowlist.size()>0){
					Row row=rowlist.get(0);
					socialrelation = getSocialRelationFromRow(row);

				}
			}
			

			if (socialrelation == null) {
				long relationId = counterLocalService.increment();

				relation = socialRelationPersistence.create(relationId);

				relation.setCompanyId(user1.getCompanyId());
				relation.setCreateDate(System.currentTimeMillis());
				relation.setUserId1(userId1);
				relation.setUserId2(userId2);
				relation.setType(type);

				socialRelationPersistence.update(relation, false);
			}

			if (SocialRelationConstants.isTypeBi(type)) {
				SocialRelation biRelation =
					socialRelationPersistence.fetchByU1_U2_T(
						userId2, userId1, type);

				if (biRelation == null) {
					long biRelationId = counterLocalService.increment();

					biRelation = socialRelationPersistence.create(biRelationId);

					biRelation.setCompanyId(user1.getCompanyId());
					biRelation.setCreateDate(System.currentTimeMillis());
					biRelation.setUserId1(userId2);
					biRelation.setUserId2(userId1);
					biRelation.setType(type);

					socialRelationPersistence.update(biRelation, false);
				}
			}

			return relation;
	 		
	 		
	 		
	 		return super.addRelation(userId1, userId2, type);
	 	}

	 	/**
	 	* Removes the relation (and its inverse in case of a bidirectional
	 	* relation) from the database.
	 	*
	 	* @param relationId the primary key of the relation
	 	* @throws PortalException if the relation could not be found
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public void deleteRelation(long relationId)
	 		throws com.liferay.portal.kernel.exception.PortalException,
	 			com.liferay.portal.kernel.exception.SystemException {
	 		super.deleteRelation(relationId);
	 	}

	 	/**
	 	* Removes the matching relation (and its inverse in case of a bidirectional
	 	* relation) from the database.
	 	*
	 	* @param userId1 the user that is the subject of the relation
	 	* @param userId2 the user at the other end of the relation
	 	* @param type the relation's type
	 	* @throws PortalException if the relation or its inverse relation (if
	 	applicable) could not be found
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public void deleteRelation(long userId1, long userId2, int type)
	 		throws com.liferay.portal.kernel.exception.PortalException,
	 			com.liferay.portal.kernel.exception.SystemException {
	 		super.deleteRelation(userId1, userId2, type);
	 	}

	 	/**
	 	* Removes the relation (and its inverse in case of a bidirectional
	 	* relation) from the database.
	 	*
	 	* @param relation the relation to be removed
	 	* @throws PortalException if the relation is bidirectional and its inverse
	 	relation could not be found
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public void deleteRelation(
	 		com.liferay.portlet.social.model.SocialRelation relation)
	 		throws com.liferay.portal.kernel.exception.PortalException,
	 			com.liferay.portal.kernel.exception.SystemException {
	 		super.deleteRelation(relation);
	 	}

	 	/**
	 	* Removes all relations involving the user from the database.
	 	*
	 	* @param userId the primary key of the user
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public void deleteRelations(long userId)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		super.deleteRelations(userId);
	 	}

	 	/**
	 	* Removes all relations between User1 and User2.
	 	*
	 	* @param userId1 the user that is the subject of the relation
	 	* @param userId2 the user at the other end of the relation
	 	* @throws PortalException if the inverse relation could not be found
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public void deleteRelations(long userId1, long userId2)
	 		throws com.liferay.portal.kernel.exception.PortalException,
	 			com.liferay.portal.kernel.exception.SystemException {
	 		super.deleteRelations(userId1, userId2);
	 	}

	 	/**
	 	* Returns a range of all the inverse relations of the given type for which
	 	* the user is User2 of the relation.
	 	*
	 	* <p>
	 	* Useful when paginating results. Returns a maximum of <code>end -
	 	* start</code> instances. <code>start</code> and <code>end</code> are not
	 	* primary keys, they are indexes in the result set. Thus, <code>0</code>
	 	* refers to the first result in the set. Setting both <code>start</code>
	 	* and <code>end</code> to {@link
	 	* com.liferay.portal.kernel.dao.orm.QueryUtil#ALL_POS} will return the full
	 	* result set.
	 	* </p>
	 	*
	 	* @param userId the primary key of the user
	 	* @param type the relation's type
	 	* @param start the lower bound of the range of results
	 	* @param end the upper bound of the range of results (not inclusive)
	 	* @return the range of matching relations
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public java.util.List<com.liferay.portlet.social.model.SocialRelation> getInverseRelations(
	 		long userId, int type, int start, int end)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.getInverseRelations(userId, type,
	 			start, end);
	 	}

	 	/**
	 	* Returns the number of inverse relations of the given type for which the
	 	* user is User2 of the relation.
	 	*
	 	* @param userId the primary key of the user
	 	* @param type the relation's type
	 	* @return the number of matching relations
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public int getInverseRelationsCount(long userId, int type)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.getInverseRelationsCount(userId, type);
	 	}

	 	/**
	 	* Returns the relation identified by its primary key.
	 	*
	 	* @param relationId the primary key of the relation
	 	* @return Returns the relation
	 	* @throws PortalException if the relation could not be found
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public com.liferay.portlet.social.model.SocialRelation getRelation(
	 		long relationId)
	 		throws com.liferay.portal.kernel.exception.PortalException,
	 			com.liferay.portal.kernel.exception.SystemException {
	 		
	 		
	 		
			BoundStatement boundStatement = new BoundStatement(getRelationStatement);
			ResultSet results=session.execute(boundStatement.bind(relationId));
			if(results!=null )		
			{
				List<Row> rowlist=results.all();
				System.out.println(rowlist.size());
				if(rowlist.size()>0){
					Row row=rowlist.get(0);
					SocialRelation socialrelation = getSocialRelationFromRow(row);
					return socialrelation;
				}
			}
			return null;	 		
	 		
	 		
	 		//return super.getRelation(relationId);
	 	}
	 	
	 	
	 	

	 	/**
	 	* Returns the relation of the given type between User1 and User2.
	 	*
	 	* @param userId1 the user that is the subject of the relation
	 	* @param userId2 the user at the other end of the relation
	 	* @param type the relation's type
	 	* @return Returns the relation
	 	* @throws PortalException if the relation could not be found
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public com.liferay.portlet.social.model.SocialRelation getRelation(
	 		long userId1, long userId2, int type)
	 		throws com.liferay.portal.kernel.exception.PortalException,
	 			com.liferay.portal.kernel.exception.SystemException {
	 		return super.getRelation(userId1, userId2, type);
	 	}

	 	/**
	 	* Returns a range of all the relations of the given type where the user is
	 	* the subject of the relation.
	 	*
	 	* <p>
	 	* Useful when paginating results. Returns a maximum of <code>end -
	 	* start</code> instances. <code>start</code> and <code>end</code> are not
	 	* primary keys, they are indexes in the result set. Thus, <code>0</code>
	 	* refers to the first result in the set. Setting both <code>start</code>
	 	* and <code>end</code> to {@link
	 	* com.liferay.portal.kernel.dao.orm.QueryUtil#ALL_POS} will return the full
	 	* result set.
	 	* </p>
	 	*
	 	* @param userId the primary key of the user
	 	* @param type the relation's type
	 	* @param start the lower bound of the range of results
	 	* @param end the upper bound of the range of results (not inclusive)
	 	* @return the range of relations
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public java.util.List<com.liferay.portlet.social.model.SocialRelation> getRelations(
	 		long userId, int type, int start, int end)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.getRelations(userId, type, start, end);
	 	}

	 	/**
	 	* Returns a range of all the relations between User1 and User2.
	 	*
	 	* <p>
	 	* Useful when paginating results. Returns a maximum of <code>end -
	 	* start</code> instances. <code>start</code> and <code>end</code> are not
	 	* primary keys, they are indexes in the result set. Thus, <code>0</code>
	 	* refers to the first result in the set. Setting both <code>start</code>
	 	* and <code>end</code> to {@link
	 	* com.liferay.portal.kernel.dao.orm.QueryUtil#ALL_POS} will return the full
	 	* result set.
	 	* </p>
	 	*
	 	* @param userId1 the user that is the subject of the relation
	 	* @param userId2 the user at the other end of the relation
	 	* @param start the lower bound of the range of results
	 	* @param end the upper bound of the range of results (not inclusive)
	 	* @return the range of relations
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public java.util.List<com.liferay.portlet.social.model.SocialRelation> getRelations(
	 		long userId1, long userId2, int start, int end)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.getRelations(userId1, userId2,
	 			start, end);
	 	}

	 	/**
	 	* Returns the number of relations of the given type where the user is the
	 	* subject of the relation.
	 	*
	 	* @param userId the primary key of the user
	 	* @param type the relation's type
	 	* @return the number of relations
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public int getRelationsCount(long userId, int type)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.getRelationsCount(userId, type);
	 	}

	 	/**
	 	* Returns the number of relations between User1 and User2.
	 	*
	 	* @param userId1 the user that is the subject of the relation
	 	* @param userId2 the user at the other end of the relation
	 	* @return the number of relations
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public int getRelationsCount(long userId1, long userId2)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.getRelationsCount(userId1, userId2);
	 	}

	 	/**
	 	* Returns <code>true</code> if a relation of the given type exists where
	 	* the user with primary key <code>userId1</code> is User1 of the relation
	 	* and the user with the primary key <code>userId2</code> is User2 of the
	 	* relation.
	 	*
	 	* @param userId1 the user that is the subject of the relation
	 	* @param userId2 the user at the other end of the relation
	 	* @param type the relation's type
	 	* @return <code>true</code> if the relation exists; <code>false</code>
	 	otherwise
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public boolean hasRelation(long userId1, long userId2, int type)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.hasRelation(userId1, userId2, type);
	 	}

	 	/**
	 	* Returns <code>true</code> if the users can be in a relation of the given
	 	* type where the user with primary key <code>userId1</code> is User1 of the
	 	* relation and the user with the primary key <code>userId2</code> is User2
	 	* of the relation.
	 	*
	 	* <p>
	 	* This method returns <code>false</code> if User1 and User2 are the same,
	 	* if either user is the default user, or if a matching relation already
	 	* exists.
	 	* </p>
	 	*
	 	* @param userId1 the user that is the subject of the relation
	 	* @param userId2 the user at the other end of the relation
	 	* @param type the relation's type
	 	* @return <code>true</code> if the two users can be in a new relation of
	 	the given type; <code>false</code> otherwise
	 	* @throws SystemException if a system exception occurred
	 	*/
	 	public boolean isRelatable(long userId1, long userId2, int type)
	 		throws com.liferay.portal.kernel.exception.SystemException {
	 		return super.isRelatable(userId1, userId2, type);
	 	}


	 	
	 	
	 	private SocialRelation getSocialRelationFromRow(Row row)
				throws SystemException {
			SocialRelation socialRelation= this.createSocialRelation(row.getLong("relatiomid"));
			socialRelation.setCompanyId(row.getLong("companyid"));
			socialRelation.setCreateDate(row.getDate("createdate").getTime());
			socialRelation.setUserId1(row.getLong("userid1"));
			socialRelation.setUserId2(row.getLong("userid2"));			
			socialRelation.setType(row.getInt("type_"));
			return socialRelation;
		}	 
	
	
	
}
