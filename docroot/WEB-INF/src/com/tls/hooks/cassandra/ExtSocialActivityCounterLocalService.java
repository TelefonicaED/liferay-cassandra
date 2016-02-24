package com.tls.hooks.cassandra;

import java.util.Date;
import java.util.List;




import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.liferay.counter.service.CounterLocalServiceUtil;
import com.liferay.portal.PortalException;
import com.liferay.portal.SystemException;
import com.liferay.portal.kernel.dao.db.DB;
import com.liferay.portal.kernel.dao.db.DBFactoryUtil;
import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.transaction.Propagation;
import com.liferay.portal.kernel.transaction.Transactional;
import com.liferay.portal.kernel.util.GetterUtil;
import com.liferay.portal.kernel.util.PropsKeys;
import com.liferay.portal.kernel.util.PropsUtil;
import com.liferay.portal.kernel.util.StringBundler;
import com.liferay.portal.kernel.util.StringPool;
import com.liferay.portal.kernel.util.StringUtil;
import com.liferay.portal.model.Group;
import com.liferay.portal.model.Lock;
import com.liferay.portal.model.User;
import com.liferay.portal.service.GroupLocalServiceUtil;
import com.liferay.portal.service.LockLocalServiceUtil;
import com.liferay.portal.service.UserLocalServiceUtil;
import com.liferay.portal.util.PortalUtil;
import com.liferay.portlet.asset.model.AssetEntry;
import com.liferay.portlet.social.model.SocialAchievement;
import com.liferay.portlet.social.model.SocialActivity;
import com.liferay.portlet.social.model.SocialActivityCounter;
import com.liferay.portlet.social.model.SocialActivityCounterConstants;
import com.liferay.portlet.social.model.SocialActivityCounterDefinition;
import com.liferay.portlet.social.model.SocialActivityDefinition;
import com.liferay.portlet.social.model.SocialActivityLimit;
import com.liferay.portlet.social.model.SocialActivityProcessor;
import com.liferay.portlet.social.service.SocialActivityCounterLocalService;
import com.liferay.portlet.social.service.SocialActivityCounterLocalServiceUtil;
import com.liferay.portlet.social.service.SocialActivityCounterLocalServiceWrapper;
import com.liferay.portlet.social.service.SocialActivityLimitLocalServiceUtil;
import com.liferay.portlet.social.service.SocialActivitySettingLocalServiceUtil;
import com.liferay.portlet.social.service.persistence.SocialActivityLimitUtil;
import com.liferay.portlet.social.util.SocialCounterPeriodUtil;

public class ExtSocialActivityCounterLocalService  extends SocialActivityCounterLocalServiceWrapper {


	Session session = ExtConexionCassandra.session;

	
  	PreparedStatement getSocialActivityCounter;	
  	PreparedStatement getSocialActivityCounterId;
  	PreparedStatement updateSocialActivityCounter_counter;  	
  	PreparedStatement updateSocialActivityCounter;
  	PreparedStatement getSocialActivityCounter_counter;
	


	   
		private void preparedStatements () {
	   
	  	 // SELECT
	      getSocialActivityCounter = session.prepare(
			      "select * from liferay.socialactivitycounter where groupid= ? and classNameId= ? and classPK=? and name= ? and ownerType = ? ALLOW FILTERING ;");

	      getSocialActivityCounter_counter = session.prepare(
			      "select * from liferay.socialactivitycounter_counter where activityCounterId= ?;");
	      
	      getSocialActivityCounterId = session.prepare(
			      "select * from liferay.socialactivitycounter where activityCounterId= ?;");
	      
	      
	      
	      //UPDATE
	      updateSocialActivityCounter = session.prepare(
			      "UPDATE liferay.socialactivitycounter SET classnameid = ?, classpk=? , companyId= ? ,groupId=?, "
			        + "name=?, ownertype=?, graceValue=?, startPeriod=?   where activityCounterId= ?;");
	      
	      updateSocialActivityCounter_counter = session.prepare(
			      "UPDATE liferay.socialactivitycounter_counter SET currentvalue=currentvalue+?, totalvalue=totalvalue+?, "
			      + "endPeriod=endPeriod+ ? where activityCounterId= ?;");
	      
		}   
	   
	  
	
	public ExtSocialActivityCounterLocalService(
			SocialActivityCounterLocalService socialActivityCounterLocalService) {
		super(socialActivityCounterLocalService);

 	    preparedStatements();
		// TODO Auto-generated constructor stub
	}
	
 

		/**
		* Adds the social activity counter to the database. Also notifies the appropriate model listeners.
		*
		* @param socialActivityCounter the social activity counter
		* @return the social activity counter that was added
		* @throws SystemException if a system exception occurred
		*/
		@Override
		public com.liferay.portlet.social.model.SocialActivityCounter addSocialActivityCounter(
			com.liferay.portlet.social.model.SocialActivityCounter socialActivityCounter)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("addSocialActivityCounter");
			return super.addSocialActivityCounter(socialActivityCounter);
		}

		/**
		* Creates a new social activity counter with the primary key. Does not add the social activity counter to the database.
		*
		* @param activityCounterId the primary key for the new social activity counter
		* @return the new social activity counter
		*/
		@Override		
		public com.liferay.portlet.social.model.SocialActivityCounter createSocialActivityCounter(
			long activityCounterId) {
			System.out.println("createSocialActivityCounter");
			return super.createSocialActivityCounter(activityCounterId);
		}

		/**
		* Deletes the social activity counter with the primary key from the database. Also notifies the appropriate model listeners.
		*
		* @param activityCounterId the primary key of the social activity counter
		* @return the social activity counter that was removed
		* @throws PortalException if a social activity counter with the primary key could not be found
		* @throws SystemException if a system exception occurred
		*/
		@Override		
		public com.liferay.portlet.social.model.SocialActivityCounter deleteSocialActivityCounter(
			long activityCounterId)
			throws com.liferay.portal.kernel.exception.PortalException,
				com.liferay.portal.kernel.exception.SystemException {
			System.out.println("deleteSocialActivityCounter1");
			return super.deleteSocialActivityCounter(activityCounterId);
		}

		/**
		* Deletes the social activity counter from the database. Also notifies the appropriate model listeners.
		*
		* @param socialActivityCounter the social activity counter
		* @return the social activity counter that was removed
		* @throws SystemException if a system exception occurred
		*/
		@Override		
		public com.liferay.portlet.social.model.SocialActivityCounter deleteSocialActivityCounter(
			com.liferay.portlet.social.model.SocialActivityCounter socialActivityCounter)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("deleteSocialActivityCounter2");
			return super.deleteSocialActivityCounter(socialActivityCounter);
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
			return super.dynamicQuery(dynamicQuery,
				start, end);
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
			return super.dynamicQuery(dynamicQuery,
				start, end, orderByComparator);
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
		@Override
		public com.liferay.portlet.social.model.SocialActivityCounter fetchSocialActivityCounter(
			long activityCounterId)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("fetchSocialActivityCounter");
			return super.fetchSocialActivityCounter(activityCounterId);
		}

		/**
		* Returns the social activity counter with the primary key.
		*
		* @param activityCounterId the primary key of the social activity counter
		* @return the social activity counter
		* @throws PortalException if a social activity counter with the primary key could not be found
		* @throws SystemException if a system exception occurred
		*/
		@Override
		public com.liferay.portlet.social.model.SocialActivityCounter getSocialActivityCounter(
			long activityCounterId)
			throws com.liferay.portal.kernel.exception.PortalException,
				com.liferay.portal.kernel.exception.SystemException {
			System.out.println("getSocialActivityCounter");
			return super.getSocialActivityCounter(activityCounterId);
		}
		@Override
		public com.liferay.portal.model.PersistedModel getPersistedModel(
			java.io.Serializable primaryKeyObj)
			throws com.liferay.portal.kernel.exception.PortalException,
				com.liferay.portal.kernel.exception.SystemException {
			System.out.println("getPersistedModel");
			return super.getPersistedModel(primaryKeyObj);
		}

		/**
		* Returns a range of all the social activity counters.
		*
		* <p>
		* Useful when paginating results. Returns a maximum of <code>end - start</code> instances. <code>start</code> and <code>end</code> are not primary keys, they are indexes in the result set. Thus, <code>0</code> refers to the first result in the set. Setting both <code>start</code> and <code>end</code> to {@link com.liferay.portal.kernel.dao.orm.QueryUtil#ALL_POS} will return the full result set.
		* </p>
		*
		* @param start the lower bound of the range of social activity counters
		* @param end the upper bound of the range of social activity counters (not inclusive)
		* @return the range of social activity counters
		* @throws SystemException if a system exception occurred
		*/
		@Override
		public java.util.List<com.liferay.portlet.social.model.SocialActivityCounter> getSocialActivityCounters(
			int start, int end)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("getSocialActivityCounters");
			return super.getSocialActivityCounters(start,
				end);
		}

		/**
		* Returns the number of social activity counters.
		*
		* @return the number of social activity counters
		* @throws SystemException if a system exception occurred
		*/
		@Override
		public int getSocialActivityCountersCount()
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("getSocialActivityCountersCount");
			return super.getSocialActivityCountersCount();
		}

		/**
		* Updates the social activity counter in the database or adds it if it does not yet exist. Also notifies the appropriate model listeners.
		*
		* @param socialActivityCounter the social activity counter
		* @return the social activity counter that was updated
		* @throws SystemException if a system exception occurred
		*/
		@Override
		public com.liferay.portlet.social.model.SocialActivityCounter updateSocialActivityCounter(
			com.liferay.portlet.social.model.SocialActivityCounter socialActivityCounter)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("updateSocialActivityCounter1");
			return super.updateSocialActivityCounter(socialActivityCounter);
		}

		/**
		* Updates the social activity counter in the database or adds it if it does not yet exist. Also notifies the appropriate model listeners.
		*
		* @param socialActivityCounter the social activity counter
		* @param merge whether to merge the social activity counter with the current session. See {@link com.liferay.portal.service.persistence.BatchSession#update(com.liferay.portal.kernel.dao.orm.Session, com.liferay.portal.model.BaseModel, boolean)} for an explanation.
		* @return the social activity counter that was updated
		* @throws SystemException if a system exception occurred
		*/
		@Override
		public com.liferay.portlet.social.model.SocialActivityCounter updateSocialActivityCounter(
			com.liferay.portlet.social.model.SocialActivityCounter socialActivityCounter,
			boolean merge)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("updateSocialActivityCounter2");
			return super.updateSocialActivityCounter(socialActivityCounter,
				merge);
		}

		/**
		* Returns the Spring bean ID for super bean.
		*
		* @return the Spring bean ID for super bean
		*/
		@Override
		public java.lang.String getBeanIdentifier() {
			System.out.println("getBeanIdentifier");
			return super.getBeanIdentifier();
		}

		/**
		* Sets the Spring bean ID for super bean.
		*
		* @param beanIdentifier the Spring bean ID for super bean
		*/
		@Override
		public void setBeanIdentifier(java.lang.String beanIdentifier) {
			System.out.println("setBeanIdentifier");
			super.setBeanIdentifier(beanIdentifier);
		}
		
		
		
		@Override
		public SocialActivityCounter addActivityCounter(
				long groupId, long classNameId, long classPK, String name,
				int ownerType, int currentValue, int totalValue, int startPeriod,
				int endPeriod)
			throws com.liferay.portal.kernel.exception.PortalException, com.liferay.portal.kernel.exception.SystemException {

			return addActivityCounter(groupId, classNameId, classPK, name, ownerType, currentValue,	
					totalValue, startPeriod, endPeriod, 0, 0);
		}
		
		

		@Override
		public SocialActivityCounter addActivityCounter(
			long groupId, long classNameId, long classPK, String name,
			int ownerType, int currentValue, int totalValue, int startPeriod,
			int endPeriod, long previousActivityCounterId, int periodLength)
			throws com.liferay.portal.kernel.exception.PortalException,
				com.liferay.portal.kernel.exception.SystemException {
			System.out.println("addActivityCounter2");
			
			SocialActivityCounter activityCounter = null;

			String lockKey = getLockKey(groupId, classNameId, classPK, name, ownerType);

			Lock lock = null;

			while (true) {
				try {
					lock = LockLocalServiceUtil.lock(SocialActivityCounter.class.getName(), lockKey, lockKey,
						false);
				}
				catch (Exception e) {
					if (_log.isWarnEnabled()) {
						_log.warn(
							"Unable to acquire activity counter lock. Retrying.");
					}

					continue;
				}

				if (lock.isNew()) {
					try {
						DB db =  DBFactoryUtil.getDB();

						String dbType = db.getType();

						if (dbType.equals(DB.TYPE_HYPERSONIC)) {

							// LPS-25408

							activityCounter = createActivityCounter(
								groupId, classNameId, classPK, name, ownerType,
								currentValue, totalValue, startPeriod, endPeriod,
								previousActivityCounterId, periodLength);
						}
						else {
							activityCounter =
									createActivityCounter(
										groupId, classNameId, classPK, name,
										ownerType, currentValue, totalValue,
										startPeriod, endPeriod,
										previousActivityCounterId, periodLength);

						}
					}
					finally {
						LockLocalServiceUtil.unlock(SocialActivityCounter.class.getName(), lockKey, lockKey,false);
					}

					break;
				}

				Date createDate = lock.getCreateDate();
				
				if ((System.currentTimeMillis() - createDate.getTime()) >=
						GetterUtil.getLong(PropsUtil.get(PropsKeys.SOCIAL_ACTIVITY_COUNTER_LOCK_TIMEOUT))) {

					LockLocalServiceUtil.unlock(
						SocialActivityCounter.class. getName(), lockKey,
						lock.getOwner(), false);

					if (_log.isWarnEnabled()) {
						_log.warn(
							"Forcibly removed lock " + lock + ". See " +
								PropsKeys.SOCIAL_ACTIVITY_COUNTER_LOCK_TIMEOUT);
					}
				}
				else {
					try {
						Thread.sleep(
								GetterUtil.getLong(PropsUtil.get(PropsKeys.SOCIAL_ACTIVITY_COUNTER_LOCK_RETRY_DELAY))) ;
					}
					catch (InterruptedException ie) {
						if (_log.isWarnEnabled()) {
							_log.warn(
								"Interrupted while waiting to reacquire lock", ie);
						}
					}
				}
			}

			return activityCounter;
			
			
		//	return super.addActivityCounter(groupId,classNameId, classPK, name, ownerType, currentValue, totalValue,
		//		startPeriod, endPeriod, previousActivityCounterId, periodLength);
		}
		@Override
		public void addActivityCounters(
			com.liferay.portlet.social.model.SocialActivity activity)
			throws com.liferay.portal.kernel.exception.PortalException,
				com.liferay.portal.kernel.exception.SystemException {

			
			  
			if (!SocialActivitySettingLocalServiceUtil.isEnabled(
					activity.getGroupId(), activity.getClassNameId())) {

				return;
			}
            
			User user = UserLocalServiceUtil.getUser(activity.getUserId());

			SocialActivityDefinition activityDefinition =
					SocialActivitySettingLocalServiceUtil.getActivityDefinition(
					activity.getGroupId(), activity.getClassName(),
					activity.getType());

			if ((activityDefinition == null) || !activityDefinition.isEnabled()) {
				return;
			}
			

			
			SocialActivityProcessor activityProcessor =		activityDefinition.getActivityProcessor();

			if (activityProcessor != null) {
				activityProcessor.processActivity(activity);
			}

			AssetEntry assetEntry = activity.getAssetEntry();
 
			
			User assetEntryUser =  UserLocalServiceUtil.getUser(assetEntry.getUserId());

			for (SocialActivityCounterDefinition activityCounterDefinition :
					activityDefinition.getActivityCounterDefinitions()) {
				
			

				if (addActivityCounter(
						user, assetEntryUser, activityCounterDefinition) &&
					checkActivityLimit(user, activity, activityCounterDefinition)) {

					incrementActivityCounter(
						activity.getGroupId(), user, activity.getAssetEntry(),
						activityCounterDefinition);
				}
			}

			for (SocialAchievement achievement :
					activityDefinition.getAchievements()) {

				achievement.processActivity(activity);
			}

			if (!user.isDefaultUser() && user.isActive()) {
				incrementActivityCounter(
					activity.getGroupId(),
					PortalUtil.getClassNameId(User.class.getName()),
					activity.getUserId(),
					SocialActivityCounterConstants.NAME_USER_ACTIVITIES,
					SocialActivityCounterConstants.TYPE_ACTOR, 1,
					SocialActivityCounterConstants.PERIOD_LENGTH_SYSTEM);
			}

			if (!assetEntryUser.isDefaultUser() && assetEntryUser.isActive()) {
				incrementActivityCounter(
					activity.getGroupId(), activity.getClassNameId(),
					activity.getClassPK(),
					SocialActivityCounterConstants.NAME_ASSET_ACTIVITIES,
					SocialActivityCounterConstants.TYPE_ASSET, 1,
					SocialActivityCounterConstants.PERIOD_LENGTH_SYSTEM);
			}
			
			
			
		//	super.addActivityCounters(activity);
		}


	
		@Transactional(propagation = Propagation.REQUIRES_NEW)
		public com.liferay.portlet.social.model.SocialActivityCounter createActivityCounter(
			long groupId, long classNameId, long classPK, java.lang.String name,
			int ownerType, int currentValue, int totalValue, int startPeriod,
			int endPeriod, long previousActivityCounterId, int periodLength)
			throws com.liferay.portal.kernel.exception.PortalException,
				com.liferay.portal.kernel.exception.SystemException {
			System.out.println("createActivityCounter1");
			
			SocialActivityCounter activityCounter = null;
			SocialActivityCounter activityCounter_Counter = null;

			if (previousActivityCounterId != 0) {
			//	activityCounter = socialActivityCounterPersistence.findByPrimaryKey(previousActivityCounterId);
				
				//Search previousActivityCounterId in socialactivityCounter
				BoundStatement boundStatement = new BoundStatement(getSocialActivityCounterId);
				ResultSet results=session.execute(boundStatement.bind(previousActivityCounterId));
				if(results!=null ){
					List<Row> rowlist=results.all();
					if(rowlist.size()>0){
						Row row=rowlist.get(0);
						activityCounter = getSocialActivityCounterFromRow(row);
					}
				}
				//Search previousActivityCounterId in socialactivityCounter_Counter
				boundStatement = new BoundStatement(getSocialActivityCounter_counter);
				results=session.execute(boundStatement.bind(previousActivityCounterId));
				if(results!=null ){
					List<Row> rowlist=results.all();

					if(rowlist.size()>0){
						Row row=rowlist.get(0);
						activityCounter_Counter = getSocialActivityCounter_CounterFromRow(row);
					}
				}				
				
					

				if (periodLength ==	SocialActivityCounterConstants.PERIOD_LENGTH_SYSTEM) {			
					activityCounter.setEndPeriod(SocialCounterPeriodUtil.getStartPeriod() - 1);
				}
				else {
					activityCounter.setEndPeriod(activityCounter_Counter.getStartPeriod() + periodLength - 1);
				}

				
				//socialActivityCounterPersistence.update(activityCounter, false);


				try {
					boundStatement = new BoundStatement(updateSocialActivityCounter_counter);
					session.execute(boundStatement.bind(Long.getLong("0") ,Long.getLong("0"),activityCounter.getEndPeriod(),activityCounter.getActivityCounterId()));				
					
				}
				catch (Exception e) {
					if (_log.isWarnEnabled()) {
						_log.warn("Unable to acquire activity counter lock. Retrying.");
					}
				}				
				
				
				
			}

		//	activityCounter = socialActivityCounterPersistence.fetchByG_C_C_N_O_E(groupId, classNameId, classPK, name, ownerType, endPeriod, false);
			
			
			//Buscamos los valores (groupId, classNameId, classPK, name, ownerType) en socialactivitycounter 
			// y despés se mira el valor de endPeriod en la tabla  socialactivitycounter_counter, si coincide, 
			// devolvemos el objeto entero socialactivitycounter
			

			SocialActivityCounter activityCounterAux1_counter = null;
			BoundStatement boundStatement = new BoundStatement(getSocialActivityCounter);
			ResultSet results=session.execute(boundStatement.bind(groupId, classNameId, classPK, name, ownerType));
			if(results!=null )		
			{
				List<Row> rowlist=results.all();

				if(rowlist.size()>0){
					Row row=rowlist.get(0);
					activityCounter = getSocialActivityCounterFromRow(row);

				}
			}			

            if (activityCounter!=null){
    			boundStatement = new BoundStatement(getSocialActivityCounter_counter);
    			results=session.execute(boundStatement.bind(activityCounter.getActivityCounterId()));
    			if(results!=null )		
    			{
    				List<Row> rowlist=results.all();

    				if(rowlist.size()>0){
    					Row row=rowlist.get(0);
    					activityCounterAux1_counter = getSocialActivityCounter_CounterFromRow(row);

    				}
    			}
            }
				
            if (activityCounterAux1_counter!= null){
            	if (endPeriod == activityCounterAux1_counter.getEndPeriod()){
            		
            		//inserta valorees los contadores
            		activityCounter.setCurrentValue(activityCounterAux1_counter.getCurrentValue());
            		activityCounter.setEndPeriod(activityCounterAux1_counter.getEndPeriod());
            		activityCounter.setGraceValue(activityCounterAux1_counter.getGraceValue());
            		activityCounter.setStartPeriod(activityCounterAux1_counter.getStartPeriod());
            		activityCounter.setTotalValue(activityCounterAux1_counter.getTotalValue());
            		
            		return activityCounter;
            	}
            }
            
            
			

			if (activityCounter != null) {
				return activityCounter;
			}

			Group group = GroupLocalServiceUtil.getGroup(groupId); 
			
			long activityCounterId = CounterLocalServiceUtil.increment();
			
			activityCounter =  SocialActivityCounterLocalServiceUtil.createSocialActivityCounter(activityCounterId);
	
			activityCounter.setGroupId(groupId);
			activityCounter.setCompanyId(group.getCompanyId());
			activityCounter.setClassNameId(classNameId);
			activityCounter.setClassPK(classPK);
			activityCounter.setName(name);
			activityCounter.setOwnerType(ownerType);
			activityCounter.setCurrentValue(currentValue);
			activityCounter.setTotalValue(totalValue);
			activityCounter.setStartPeriod(startPeriod);
			activityCounter.setEndPeriod(endPeriod);

			
			//socialActivityCounterPersistence.update(activityCounter, false);
			
			//inserta o actualiza en SocialActivityCounter y en SocialActivityCounter_Counter
			
			boundStatement = new BoundStatement(updateSocialActivityCounter);
			session.execute(boundStatement.bind(
					activityCounter.getClassNameId(),
					activityCounter.getClassPK(),
					activityCounter.getCompanyId(),
					activityCounter.getGroupId(),
					activityCounter.getName(),
					activityCounter.getOwnerType(),
					activityCounter.getGraceValue(),
					activityCounter.getStartPeriod(),
					activityCounter.getActivityCounterId()
					));				
			
			
	
			
			try {
				boundStatement = new BoundStatement(updateSocialActivityCounter_counter);
				session.execute(boundStatement.bind(
						activityCounter.getCurrentValue(),
						activityCounter.getTotalValue(),			
						activityCounter.getEndPeriod(),
						activityCounter.getActivityCounterId()

						));				
				
			}
			catch (Exception e) {
				if (_log.isWarnEnabled()) {
					_log.warn("Unable to acquire activity counter lock. Retrying.");
				}
			}				
			
	
								


			return activityCounter;
			
			
			
		//	return super.createActivityCounter(groupId,
		//		classNameId, classPK, name, ownerType, currentValue, totalValue,
		//		startPeriod, endPeriod, previousActivityCounterId, periodLength);
		}
		@Override
		public void deleteActivityCounters(
			com.liferay.portlet.asset.model.AssetEntry assetEntry)
			throws com.liferay.portal.kernel.exception.PortalException,
				com.liferay.portal.kernel.exception.SystemException {
			System.out.println("deleteActivityCounters1");
			super.deleteActivityCounters(assetEntry);
		}
		@Override
		public void deleteActivityCounters(long classNameId, long classPK)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("deleteActivityCounters2");
			super.deleteActivityCounters(classNameId,
				classPK);
		}
		@Override
		public com.liferay.portlet.social.model.SocialActivityCounter fetchActivityCounterByEndPeriod(
			long groupId, long classNameId, long classPK, java.lang.String name,
			int ownerType, int endPeriod)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("fetchActivityCounterByEndPeriod1");
			return super.fetchActivityCounterByEndPeriod(groupId,
				classNameId, classPK, name, ownerType, endPeriod);
		}
		@Override
		public com.liferay.portlet.social.model.SocialActivityCounter fetchActivityCounterByStartPeriod(
			long groupId, long classNameId, long classPK, java.lang.String name,
			int ownerType, int startPeriod)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("fetchActivityCounterByEndPeriod2");
			return super.fetchActivityCounterByStartPeriod(groupId,
				classNameId, classPK, name, ownerType, startPeriod);
		}
		@Override
		public com.liferay.portlet.social.model.SocialActivityCounter fetchLatestActivityCounter(
			long groupId, long classNameId, long classPK, java.lang.String name,
			int ownerType)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("fetchLatestActivityCounter");
			

			BoundStatement boundStatement = new BoundStatement(getSocialActivityCounter);
			ResultSet results=session.execute(boundStatement.bind(groupId,classNameId,classPK,name,ownerType));
			if(results!=null ){
				List<Row> rowlist=results.all();
				if(rowlist.size()>0){
					Row row=rowlist.get(0);
					SocialActivityCounter activityCounter = getSocialActivityCounterFromRow(row);
					return activityCounter;
	
				}
			}			
			
			return null;
			//return super.fetchLatestActivityCounter(groupId,classNameId, classPK, name, ownerType);
		}
		@Override
		public java.util.List<com.liferay.portlet.social.model.SocialActivityCounter> getOffsetActivityCounters(
			long groupId, java.lang.String name, int startOffset, int endOffset)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("getOffsetActivityCounters");
			return super.getOffsetActivityCounters(groupId,
				name, startOffset, endOffset);
		}
		@Override
		public java.util.List<com.liferay.portlet.social.model.SocialActivityCounter> getOffsetDistributionActivityCounters(
			long groupId, java.lang.String name, int startOffset, int endOffset)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("getOffsetDistributionActivityCounters1");
			return super.getOffsetDistributionActivityCounters(groupId,
				name, startOffset, endOffset);
		}
		@Override
		public java.util.List<com.liferay.portlet.social.model.SocialActivityCounter> getPeriodActivityCounters(
			long groupId, java.lang.String name, int startPeriod, int endPeriod)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("getPeriodActivityCounters");
			return super.getPeriodActivityCounters(groupId,
				name, startPeriod, endPeriod);
		}
		@Override
		public java.util.List<com.liferay.portlet.social.model.SocialActivityCounter> getPeriodDistributionActivityCounters(
			long groupId, java.lang.String name, int startPeriod, int endPeriod)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("getPeriodDistributionActivityCounters2");
			return super.getPeriodDistributionActivityCounters(groupId,
				name, startPeriod, endPeriod);
		}
		@Override
		public java.util.List<com.liferay.portal.kernel.util.Tuple> getUserActivityCounters(
			long groupId, java.lang.String[] rankingNames,
			java.lang.String[] selectedNames, int start, int end)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("getUserActivityCounters");
			return super.getUserActivityCounters(groupId,
				rankingNames, selectedNames, start, end);
		}
		@Override
		public int getUserActivityCountersCount(long groupId,
			java.lang.String[] rankingNames)
			throws com.liferay.portal.kernel.exception.SystemException {
			System.out.println("getUserActivityCountersCount");
			return super.getUserActivityCountersCount(groupId,
				rankingNames);
		}
		@Override
		public void incrementUserAchievementCounter(long userId, long groupId)
			throws com.liferay.portal.kernel.exception.PortalException,
				com.liferay.portal.kernel.exception.SystemException {
			System.out.println("incrementUserAchievementCounter");
			super.incrementUserAchievementCounter(userId,
				groupId);
		}



		protected boolean addActivityCounter(
				User user, User assetEntryUser,
				SocialActivityCounterDefinition activityCounterDefinition) {

				if ((user.isDefaultUser() || !user.isActive()) &&
					(activityCounterDefinition.getOwnerType() != 
						SocialActivityCounterConstants.TYPE_ASSET)) {

					return false;
				}

				if ((assetEntryUser.isDefaultUser() || !assetEntryUser.isActive()) &&
					(activityCounterDefinition.getOwnerType() !=
						SocialActivityCounterConstants.TYPE_ACTOR)) {

					return false;
				}

				if (!activityCounterDefinition.isEnabled() ||
					(activityCounterDefinition.getIncrement() == 0)) {

					return false;
				}

				String name = activityCounterDefinition.getName();

				if ((user.getUserId() == assetEntryUser.getUserId()) &&
					(name.equals(SocialActivityCounterConstants.NAME_CONTRIBUTION) ||
					 name.equals(SocialActivityCounterConstants.NAME_POPULARITY))) {

					return false;
				}

				return true;
			}


		protected boolean checkActivityLimit(
				User user, SocialActivity activity,
				SocialActivityCounterDefinition activityCounterDefinition)
			throws com.liferay.portal.kernel.exception.SystemException, com.liferay.portal.kernel.exception.PortalException {

			if (activityCounterDefinition.getLimitValue() == 0) {
				return true;
			}

			long classPK = activity.getClassPK();

			String name = activityCounterDefinition.getName();

			if (name.equals(SocialActivityCounterConstants.NAME_PARTICIPATION)) {
				classPK = 0;
			}

			SocialActivityLimit activityLimit =
					
	
					SocialActivityLimitUtil.fetchByG_U_C_C_A_A(
					activity.getGroupId(), user.getUserId(),
					activity.getClassNameId(), classPK, activity.getType(),
					activityCounterDefinition.getName());

			if (activityLimit == null) {
				try {
					activityLimit = 
							SocialActivityLimitLocalServiceUtil.addActivityLimit(
							user.getUserId(), activity.getGroupId(),
							activity.getClassNameId(), classPK, activity.getType(),
							activityCounterDefinition.getName(),
							activityCounterDefinition.getLimitPeriod());
				}
				catch (SystemException se) {
					activityLimit =
							SocialActivityLimitUtil.fetchByG_U_C_C_A_A(
							activity.getGroupId(), user.getUserId(),
							activity.getClassNameId(), classPK, activity.getType(),
							activityCounterDefinition.getName());

					if (activityLimit == null) {
						throw se;
					}
				}
			}

			int count = activityLimit.getCount(
				activityCounterDefinition.getLimitPeriod());

			if (count < activityCounterDefinition.getLimitValue()) {
				activityLimit.setCount(
					activityCounterDefinition.getLimitPeriod(), count + 1);
                
				SocialActivityLimitLocalServiceUtil.updateSocialActivityLimit(activityLimit, false);
				

				return true;
			}

			return false;
		}

		protected void incrementActivityCounter(
				long groupId, User user, AssetEntry assetEntry,
				SocialActivityCounterDefinition activityCounterDefinition)
			throws com.liferay.portal.kernel.exception.PortalException, com.liferay.portal.kernel.exception.SystemException {
			
			int ownerType = activityCounterDefinition.getOwnerType();
			long userClassNameId = PortalUtil.getClassNameId(User.class.getName());

			if (ownerType == SocialActivityCounterConstants.TYPE_ACTOR) {
				incrementActivityCounter(
					groupId, userClassNameId, user.getUserId(),
					activityCounterDefinition.getName(), ownerType,
					activityCounterDefinition.getIncrement(),
					activityCounterDefinition.getPeriodLength());
			}
			else if (ownerType == SocialActivityCounterConstants.TYPE_ASSET) {
				incrementActivityCounter(
					groupId, assetEntry.getClassNameId(), assetEntry.getClassPK(),
					activityCounterDefinition.getName(), ownerType,
					activityCounterDefinition.getIncrement(),
					activityCounterDefinition.getPeriodLength());
			}
			else {
				incrementActivityCounter(
					groupId, userClassNameId, assetEntry.getUserId(),
					activityCounterDefinition.getName(), ownerType,
					activityCounterDefinition.getIncrement(),
					activityCounterDefinition.getPeriodLength());
			}
			


/*			
			
			if (name == null) {
				query.append(_FINDER_COLUMN_G_C_A_N_NAME_1);
			}
			else {
				if (name.equals(StringPool.BLANK)) {
					query.append(_FINDER_COLUMN_G_C_A_N_NAME_3);
				}
				else {
					query.append(_FINDER_COLUMN_G_C_A_N_NAME_2);
				}
			}
			
			
			SocialActivityCounter activityCounter = fetchLatestActivityCounter(
				groupId, classNameId, classPK, name, ownerType);
			

			if (activityCounter == null) {
				activityCounter = addActivityCounter(
					groupId, classNameId, classPK, name, ownerType, 0, 0,
					SocialCounterPeriodUtil.getStartPeriod(),
					SocialActivityCounterConstants.END_PERIOD_UNDEFINED);

				if (periodLength > 0) {
					activityCounter.setStartPeriod(
						SocialCounterPeriodUtil.getActivityDay());
				}
			}

			if (!activityCounter.isActivePeriod(periodLength)) {
				activityCounter = addActivityCounter(
					activityCounter.getGroupId(), activityCounter.getClassNameId(),
					activityCounter.getClassPK(), activityCounter.getName(),
					activityCounter.getOwnerType(), 0,
					activityCounter.getTotalValue(),
					SocialCounterPeriodUtil.getStartPeriod(),
					SocialActivityCounterConstants.END_PERIOD_UNDEFINED,
					activityCounter.getActivityCounterId(), periodLength);
			}

			activityCounter.setCurrentValue(
				activityCounter.getCurrentValue() + increment);
			activityCounter.setTotalValue(
				activityCounter.getTotalValue() + increment);

			socialActivityCounterPersistence.update(activityCounter, false);
			
*/			
		}
		

		protected void incrementActivityCounter(
				long groupId, long classNameId, long classPK, String name,
				int ownerType, int increment, int periodLength)
			throws com.liferay.portal.kernel.exception.PortalException, com.liferay.portal.kernel.exception.SystemException {

			SocialActivityCounter activityCounter = fetchLatestActivityCounter(
				groupId, classNameId, classPK, name, ownerType);

			if (activityCounter == null) {
				activityCounter = addActivityCounter(
					groupId, classNameId, classPK, name, ownerType, 0, 0,
					SocialCounterPeriodUtil.getStartPeriod(),
					SocialActivityCounterConstants.END_PERIOD_UNDEFINED);

				if (periodLength > 0) {
					activityCounter.setStartPeriod(
						SocialCounterPeriodUtil.getActivityDay());
				}
			}

			if (!activityCounter.isActivePeriod(periodLength)) {
				activityCounter = addActivityCounter(
					activityCounter.getGroupId(), activityCounter.getClassNameId(),
					activityCounter.getClassPK(), activityCounter.getName(),
					activityCounter.getOwnerType(), 0,
					activityCounter.getTotalValue(),
					SocialCounterPeriodUtil.getStartPeriod(),
					SocialActivityCounterConstants.END_PERIOD_UNDEFINED,
					activityCounter.getActivityCounterId(), periodLength);
			}

			activityCounter.setCurrentValue(activityCounter.getCurrentValue() + increment);
			activityCounter.setTotalValue(activityCounter.getTotalValue() + increment);
			
			BoundStatement boundStatement = new BoundStatement(updateSocialActivityCounter_counter);
			session.execute(boundStatement.bind(
					Long.valueOf(increment),
					Long.valueOf(increment),
					Long.valueOf(0),
					activityCounter.getActivityCounterId()
					));			

			//socialActivityCounterPersistence.update(activityCounter, false);
			
			boundStatement = new BoundStatement(updateSocialActivityCounter);
			session.execute(boundStatement.bind(
					activityCounter.getClassNameId(),
					activityCounter.getClassPK(),
					activityCounter.getCompanyId(),
					activityCounter.getGroupId(),
					activityCounter.getName(),
					activityCounter.getOwnerType(),
					activityCounter.getGraceValue(),
					activityCounter.getStartPeriod(),
					activityCounter.getActivityCounterId()
					));				
		}
		


		private SocialActivityCounter getSocialActivityCounterFromRow(Row row)
				throws SystemException {

			SocialActivityCounter activityCounter= this.createSocialActivityCounter(row.getLong("activityCounterId"));
			activityCounter.setClassNameId(row.getLong("classnameid"));
			activityCounter.setClassPK(row.getLong("classpk"));
			activityCounter.setCompanyId(row.getLong("companyid"));
			activityCounter.setGroupId(row.getLong("groupid"));
			activityCounter.setName(row.getString("name"));
			activityCounter.setOwnerType(row.getInt("ownertype"));
			return activityCounter;
		}
		     
		     
		private SocialActivityCounter getSocialActivityCounter_CounterFromRow(Row row)
				throws SystemException {

			
				SocialActivityCounter activityCounter_Counter= this.createSocialActivityCounter(row.getLong("activityCounterId"));
     	
				activityCounter_Counter.setCurrentValue((int)row.getLong("currentvalue"));
				activityCounter_Counter.setEndPeriod((int)row.getLong("endperiod"));
				//activityCounter_Counter.setStartPeriod((int)row.getLong("startperiod"));
				activityCounter_Counter.setTotalValue((int)row.getLong("totalvalue"));			
				
			

			return activityCounter_Counter;
		}		
		
		protected String getLockKey(
				long groupId, long classNameId, long classPK, String name,
				int ownerType) {

				StringBundler sb = new StringBundler(7);

				sb.append(StringUtil.toHexString(groupId));
				sb.append(StringPool.POUND);
				sb.append(StringUtil.toHexString(classNameId));
				sb.append(StringPool.POUND);
				sb.append(StringUtil.toHexString(classPK));
				sb.append(StringPool.POUND);
				sb.append(name);

				return sb.toString();
			}
		private static Log _log = LogFactoryUtil.getLog(
				SocialActivityCounterLocalService.class);

}
