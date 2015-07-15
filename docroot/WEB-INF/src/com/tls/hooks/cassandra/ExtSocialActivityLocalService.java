package com.tls.hooks.cassandra;

import java.util.Date;
import java.util.List;

import org.omg.CORBA.portable.ValueOutputStream;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.liferay.counter.service.CounterLocalServiceUtil;
import com.liferay.portal.kernel.exception.PortalException;
import com.liferay.portal.kernel.exception.SystemException;
import com.liferay.portal.kernel.lar.ImportExportThreadLocal;
import com.liferay.portal.model.Group;
import com.liferay.portal.model.GroupModel;
import com.liferay.portal.model.Layout;
import com.liferay.portal.model.User;
import com.liferay.portal.service.ClassNameLocalServiceUtil;
import com.liferay.portal.service.GroupLocalServiceUtil;
import com.liferay.portal.service.GroupService;
import com.liferay.portal.service.GroupServiceUtil;
import com.liferay.portal.service.LayoutLocalServiceUtil;
import com.liferay.portal.service.OrganizationLocalServiceUtil;
import com.liferay.portal.service.UserGroupLocalServiceUtil;
import com.liferay.portal.service.UserGroupRoleServiceUtil;
import com.liferay.portal.service.UserLocalServiceUtil;
import com.liferay.portal.service.UserServiceUtil;
import com.liferay.portal.service.permission.GroupPermissionUtil;
import com.liferay.portal.service.persistence.GroupFinderUtil;
import com.liferay.portal.service.persistence.OrganizationUtil;
import com.liferay.portal.service.persistence.UserGroupFinderUtil;
import com.liferay.portal.util.PortalUtil;
import com.liferay.portlet.asset.model.AssetEntry;
import com.liferay.portlet.asset.service.AssetEntryLocalServiceUtil;
import com.liferay.portlet.social.model.SocialActivity;
import com.liferay.portlet.social.model.SocialActivityDefinition;
import com.liferay.portlet.social.model.SocialRequestWrapper;
import com.liferay.portlet.social.service.SocialActivityCounterLocalServiceUtil;
import com.liferay.portlet.social.service.SocialActivityLocalServiceUtil;
import com.liferay.portlet.social.service.SocialActivityLocalServiceWrapper;
import com.liferay.portlet.social.service.SocialActivityLocalService;
import com.liferay.portlet.social.service.SocialActivitySettingLocalServiceUtil;

public class ExtSocialActivityLocalService extends SocialActivityLocalServiceWrapper {
	/* (non-Java-doc)
	 * @see com.liferay.portlet.social.service.SocialActivityLocalServiceWrapper#SocialActivityLocalServiceWrapper(SocialActivityLocalService socialActivityLocalService)
	 */
	private static Cluster cluster;
	private  Session session;
	static String node="127.0.0.1";
	
	
	
  	PreparedStatement insertStatement;
	PreparedStatement getGroupActivitiesStatement;
	PreparedStatement getActivityStatement;
	PreparedStatement getActivitiesStatement;
	PreparedStatement getActivitiesLimitStatement;

	
	PreparedStatement getActivitiesClassNameClasspkStatament;
	PreparedStatement getActivitiesMirrorActivityIdClassNameIdClasspkStatament;
	PreparedStatement getActivitiesMirrorActivityIdClassNameIdClasspkCountStatament;
	PreparedStatement getActivitiesreceiverUserIdStatement;
	PreparedStatement getActivitiesClassNameIdStatement;	
	PreparedStatement getActivitiesClassNameIdCountStatement;	
	PreparedStatement getActivitiesCountStatement;
	PreparedStatement getOrganizationUsersActivitiesStatement;
	PreparedStatement getOrganizationUsersActivitiesCountStatement;
	
	PreparedStatement getOrganizationActivitiesStatement;
	PreparedStatement getOrganizationActivitiesCountStatement;
	
	
	PreparedStatement getGroupUsersActivitiesStatement;
	PreparedStatement getGroupUsersActivitiesCountStatement;	
	PreparedStatement getMirrorActivityStatement;
	PreparedStatement deleteActivityStatement;

	
	
	
	
      	
	
	
	
	   public Session getSession() 
	   {
	      return this.session;
	   }
	   public void createSchema() {
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
		                  "PRIMARY KEY (activityId)" + 
		                  ");");

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
			            "CREATE INDEX IF NOT EXISTS sagi5 on liferay.socialactivity (userId);"
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
		  	 getGroupActivitiesStatement = session.prepare(
				      "select * from liferay.socialactivity where groupid= ? limit ?;");
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
	public ExtSocialActivityLocalService(SocialActivityLocalService socialActivityLocalService) {
		super(socialActivityLocalService);
		connect();
	}
	@Override
	public SocialActivity addSocialActivity(SocialActivity socialActivity)
			throws SystemException {
		System.out.println("sa");
		return super.addSocialActivity(socialActivity);
		
	}
	@Override
	public SocialActivity createSocialActivity(long activityId) {
		System.out.println("createSocialActivity");

		return super.createSocialActivity(activityId);
	}
	@Override
	public SocialActivity deleteSocialActivity(long activityId)
			throws PortalException, SystemException {
		System.out.println("deleteSocialActivity");
		System.out.println("NO IMPLEMENTADO");
		return super.deleteSocialActivity(activityId);
	}
	@Override
	public SocialActivity deleteSocialActivity(SocialActivity socialActivity)
			throws SystemException {
		System.out.println("deleteSocialActivity");
		System.out.println("NO IMPLEMENTADO");
		return super.deleteSocialActivity(socialActivity);
	}
	@Override
	public SocialActivity fetchSocialActivity(long activityId)
			throws SystemException {
		System.out.println("fetchSocialActivity");
		System.out.println("NO IMPLEMENTADO");
		return super.fetchSocialActivity(activityId);
	}
	@Override
	public SocialActivity getSocialActivity(long activityId)
			throws PortalException, SystemException {
		System.out.println("getSocialActivity");
		System.out.println("NO IMPLEMENTADO");
		return this.getActivity(activityId);
	}
	@Override
	public List<SocialActivity> getSocialActivities(int start, int end)
			throws SystemException {
		System.out.println("getSocialActivities");
		System.out.println("NO IMPLEMENTADO");
		return super.getSocialActivities(start, end);
	}
	@Override
	public int getSocialActivitiesCount() throws SystemException {
		System.out.println("getSocialActivitiesCount");
		System.out.println("NO IMPLEMENTADO");
		return super.getSocialActivitiesCount();
	}
	@Override
	public SocialActivity updateSocialActivity(SocialActivity socialActivity)
			throws SystemException {
		System.out.println("updateSocialActivity");
		System.out.println("NO IMPLEMENTADO");
		return super.updateSocialActivity(socialActivity);
	}
	@Override
	public SocialActivity updateSocialActivity(SocialActivity socialActivity,
			boolean merge) throws SystemException {
		System.out.println("updateSocialActivity");
		System.out.println("NO IMPLEMENTADO");
		return super.updateSocialActivity(socialActivity, merge);
	}
	@Override
	public void addActivity(long userId, long groupId, Date createDate,
			String className, long classPK, int type, String extraData,
			long receiverUserId) throws PortalException, SystemException {
		System.out.println("addActivity1");
		
	 	
		
//CASSANDRA

		
		
		
	
		
		

		
		if (ImportExportThreadLocal.isImportInProcess()) {
			return;
		}
		User user = UserLocalServiceUtil.getUser(userId);
		long classNameId = PortalUtil.getClassNameId(className);

		if (groupId > 0) {
			Group group = GroupLocalServiceUtil.getGroup(groupId);

			if (group.isLayout()) {
				Layout layout = LayoutLocalServiceUtil.getLayout(
					group.getClassPK());

				groupId = layout.getGroupId();
			}
		}

		SocialActivity activity = this.createSocialActivity(0);

		activity.setGroupId(groupId);
		activity.setCompanyId(user.getCompanyId());
		activity.setUserId(user.getUserId());
		activity.setCreateDate(createDate.getTime());
		activity.setMirrorActivityId(0);
		activity.setClassNameId(classNameId);
		activity.setClassPK(classPK);
		activity.setType(type);
		activity.setExtraData(extraData);
		activity.setReceiverUserId(receiverUserId);

		AssetEntry assetEntry = AssetEntryLocalServiceUtil.fetchEntry(className, classPK);
			

		activity.setAssetEntry(assetEntry);

		SocialActivity mirrorActivity = null;

		if ((receiverUserId > 0) && (userId != receiverUserId)) {
			mirrorActivity = this.createSocialActivity(0);

			mirrorActivity.setGroupId(groupId);
			mirrorActivity.setCompanyId(user.getCompanyId());
			mirrorActivity.setUserId(receiverUserId);
			mirrorActivity.setCreateDate(createDate.getTime());
			mirrorActivity.setClassNameId(classNameId);
			mirrorActivity.setClassPK(classPK);
			mirrorActivity.setType(type);
			mirrorActivity.setExtraData(extraData);
			mirrorActivity.setReceiverUserId(user.getUserId());
			mirrorActivity.setAssetEntry(assetEntry);
		}

		this.addActivity(activity, mirrorActivity);
	}
	@Override
	public void addActivity(long userId, long groupId, String className,
			long classPK, int type, String extraData, long receiverUserId)
			throws PortalException, SystemException {
		// TODO Auto-generated method stub
		System.out.println("addActivity2");
		
		Date createDate=new Date(System.currentTimeMillis());
	    this.addActivity(userId, groupId, createDate, className, classPK, type, extraData, receiverUserId);
	}
	@Override
	public void addActivity(SocialActivity activity,
			SocialActivity mirrorActivity) throws PortalException,
			SystemException {
		System.out.println("addActivity3");
		
		// TODO Auto-generated method stub
		if (ImportExportThreadLocal.isImportInProcess()) {
			return;
		}

		if ((activity.getActivityId() > 0) ||
			((mirrorActivity != null) &&
			 (mirrorActivity.getActivityId() > 0))) {

			throw new PortalException(
				"Activity and mirror activity must not have primary keys set");
		}

		SocialActivityDefinition activityDefinition =
			SocialActivitySettingLocalServiceUtil.getActivityDefinition(
				activity.getGroupId(), activity.getClassName(),
				activity.getType());
	
		if (((activityDefinition == null) && (activity.getType() < 10000)) ||
			((activityDefinition != null) &&
				activityDefinition.isLogActivity())) 
		{

			long activityId = CounterLocalServiceUtil.increment(
				SocialActivity.class.getName());

			activity.setActivityId(activityId);
			//socialActivityPersistence.update(activity, false);
			BoundStatement boundStatement = new BoundStatement(insertStatement);
			session.execute(boundStatement.bind(
					activity.getActivityId(),
					activity.getGroupId(),
					activity.getCompanyId(),
					activity.getUserId(),
					new Date(activity.getCreateDate()),
					activity.getMirrorActivityId(),
					activity.getClassNameId(),
					activity.getClassPK(),
					activity.getType(),
					activity.getExtraData(),
					activity.getReceiverUserId()
					));
			if (mirrorActivity != null) {
				long mirrorActivityId = CounterLocalServiceUtil.increment(
					SocialActivity.class.getName());

				mirrorActivity.setActivityId(mirrorActivityId);
				mirrorActivity.setMirrorActivityId(activity.getPrimaryKey());
			  boundStatement = new BoundStatement(insertStatement);
				session.execute(boundStatement.bind(
						mirrorActivity.getActivityId(),
						mirrorActivity.getGroupId(),
						mirrorActivity.getCompanyId(),
						mirrorActivity.getUserId(),
						new Date(mirrorActivity.getCreateDate()),
						mirrorActivity.getMirrorActivityId(),
						mirrorActivity.getClassNameId(),
						mirrorActivity.getClassPK(),
						mirrorActivity.getType(),
						mirrorActivity.getExtraData(),
						mirrorActivity.getReceiverUserId()
						));
			}
		}

		SocialActivityCounterLocalServiceUtil.addActivityCounters(activity);
	}
	@Override
	public void addUniqueActivity(long userId, long groupId, Date createDate,
			String className, long classPK, int type, String extraData,
			long receiverUserId) throws PortalException, SystemException {
		System.out.println("addUniqueActivity");
		
		super.addUniqueActivity(userId, groupId, createDate, className, classPK, type,
				extraData, receiverUserId);
	}
	@Override
	public void addUniqueActivity(long userId, long groupId, String className,
			long classPK, int type, String extraData, long receiverUserId)
			throws PortalException, SystemException {
		System.out.println("addUniqueActivity");
		
		super.addUniqueActivity(userId, groupId, className, classPK, type, extraData,
				receiverUserId);
	}
	@Override
	public void deleteActivities(AssetEntry assetEntry) throws PortalException,
			SystemException {
		System.out.println("deleteActivities1");
			
		this.deleteActivities(assetEntry.getClassName(), assetEntry.getClassPK());
		
		//super.deleteActivities(assetEntry);
	}
	@Override
	public void deleteActivities(String className, long classPK)
			throws SystemException {
		System.out.println("deleteActivities2");
		
		
		BoundStatement boundStatement = new BoundStatement(getActivitiesClassNameClasspkStatament);
		ResultSet results=session.execute(boundStatement.bind(ClassNameLocalServiceUtil.getClassNameId(className),classPK));
		List<Row> rows=results.all();
		if(rows.size()>0&&rows.size()>=0){
			for(int i=0;i<rows.size();i++){
				Row row=rows.get(i);
				boundStatement = new BoundStatement(deleteActivityStatement);
				session.execute(boundStatement.bind(row.getLong(0)));
				
			}
		}	
		
	//	super.deleteActivities(, classPK);
	}
	@Override
	public void deleteActivity(long activityId) throws PortalException,
			SystemException {
		System.out.println("deleteActivity");
		
		//Delete mirrorActivity
		BoundStatement boundStatement = new BoundStatement(getMirrorActivityStatement);
		ResultSet results=session.execute(boundStatement.bind(activityId));
		if(results!=null ){
			List<Row> rowlist=results.all();
			if(rowlist.size()>0){
				Row row=rowlist.get(0);
				SocialActivity socialActivity = getSocialActivityFromRow(row);
				boundStatement = new BoundStatement(deleteActivityStatement);
				session.execute(boundStatement.bind(socialActivity.getActivityId()));
			}
		}		
		
		//Delete activityId
		boundStatement = new BoundStatement(getActivityStatement);
		results=session.execute(boundStatement.bind(activityId));
		if(results!=null ){
			List<Row> rowlist=results.all();
			if(rowlist.size()>0){
				Row row=rowlist.get(0);
				SocialActivity socialActivity = getSocialActivityFromRow(row);
				boundStatement = new BoundStatement(deleteActivityStatement);
				session.execute(boundStatement.bind(socialActivity.getActivityId()));
			}
		}		

		
	//	super.deleteActivity(activityId);
	}
	@Override
	public void deleteActivity(SocialActivity activity) throws SystemException {
		// TODO Auto-generated method stub
		System.out.println("deleteActivity");
		try {
			this.deleteActivity(activity.getActivityId());
		} catch (PortalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//super.deleteActivity(activity);
	}
	@Override
	public void deleteUserActivities(long userId) throws SystemException {
		System.out.println("deleteUserActivities");
		
		
		//Delete ReciveUserId Activity
		BoundStatement boundStatement = new BoundStatement(getActivitiesreceiverUserIdStatement);
		ResultSet results=session.execute(boundStatement.bind(userId));
		if(results!=null ){
			List<Row> rowlist=results.all();
			if(rowlist.size()>0){
				Row row=rowlist.get(0);
				boundStatement = new BoundStatement(deleteActivityStatement);
				session.execute(boundStatement.bind(row.getLong(0)));
			}
		}			
		
		
		//Delete activitiesId  User 
		boundStatement = new BoundStatement(getActivitiesStatement);
		results=session.execute(boundStatement.bind(userId));
		if(results!=null ){
			List<Row> rowlist=results.all();
			if(rowlist.size()>0){
				Row row=rowlist.get(0);
				boundStatement = new BoundStatement(deleteActivityStatement);
				session.execute(boundStatement.bind(row.getLong(0)));
			}
		}			
		
		
		
		//super.deleteUserActivities(userId);
	}
	@Override
	public List<SocialActivity> getActivities(long classNameId, int start,
			int end) throws SystemException {
		System.out.println("getActivities1");
		
		List<SocialActivity> Activities=new java.util.ArrayList<SocialActivity>();
		BoundStatement boundStatement = new BoundStatement(getActivitiesClassNameIdStatement);
		boundStatement.setFetchSize(end);
		ResultSet results=session.execute(boundStatement.bind(classNameId,end));
		List<Row> rows=results.all();
		if(rows.size()>0&&rows.size()>=start){
			for(int i=start;i<rows.size();i++){
				Row row=rows.get(i);
				SocialActivity socialActivity = getSocialActivityFromRow(row);
				Activities.add(socialActivity);
			}
		}
		return Activities;		
		//return super.getActivities(classNameId, start, end);
	}
	
	@Override
	public List<SocialActivity> getActivities(long mirrorActivityId,
			long classNameId, long classPK, int start, int end)
			throws SystemException {
		System.out.println("getActivities2");
		
		List<SocialActivity> Activities=new java.util.ArrayList<SocialActivity>();
		BoundStatement boundStatement = new BoundStatement(getActivitiesMirrorActivityIdClassNameIdClasspkStatament);
		boundStatement.setFetchSize(end);
		ResultSet results=session.execute(boundStatement.bind(classNameId,end));
		List<Row> rows=results.all();
		if(rows.size()>0&&rows.size()>=start){
			for(int i=start;i<rows.size();i++){
				Row row=rows.get(i);
				SocialActivity socialActivity = getSocialActivityFromRow(row);
				Activities.add(socialActivity);
			}
		}
		return Activities;		
		
		
	//	return super.getActivities(mirrorActivityId, classNameId, classPK, start, end);
	}
	@Override
	public List<SocialActivity> getActivities(long mirrorActivityId,
			String className, long classPK, int start, int end)
			throws SystemException {
		System.out.println("getActivities3");

		List<SocialActivity> Activities = this.getActivities(mirrorActivityId, ClassNameLocalServiceUtil.getClassNameId(className),classPK, start, end);
		return Activities;
	}
	@Override
	public List<SocialActivity> getActivities(String className, int start,
			int end) throws SystemException {
		System.out.println("getActivities4");
		
		List<SocialActivity> Activities = this.getActivities(ClassNameLocalServiceUtil.getClassNameId(className), start, end);
		return Activities;

	}
	@Override
	public int getActivitiesCount(long classNameId) throws SystemException {
		System.out.println("getActivitiesCount");
	

		BoundStatement boundStatement = new BoundStatement(getActivitiesClassNameIdCountStatement);
		ResultSet results=session.execute(boundStatement.bind(classNameId));
		List<Row> rows=results.all();
		int cont =0;
		cont = (int) rows.get(0).getLong(0);
		return cont; 
		
	//	return super.getActivitiesCount(classNameId);
	}
	@Override
	public int getActivitiesCount(long mirrorActivityId, long classNameId,
			long classPK) throws SystemException {
		System.out.println("getActivitiesCount2");

		BoundStatement boundStatement = new BoundStatement(getActivitiesMirrorActivityIdClassNameIdClasspkCountStatament);
		ResultSet results=session.execute(boundStatement.bind(classNameId));
		List<Row> rows=results.all();
		int cont =0;
		cont = (int) rows.get(0).getLong(0);
		return cont; 
		
		
	}

	@Override
	public int getActivitiesCount(long mirrorActivityId, String className,
			long classPK) throws SystemException {
		// TODO Auto-generated method stub

		BoundStatement boundStatement = new BoundStatement(getActivitiesMirrorActivityIdClassNameIdClasspkCountStatament);
		ResultSet results=session.execute(boundStatement.bind(mirrorActivityId, ClassNameLocalServiceUtil.getClassName(className).getClassNameId(),classPK));
		List<Row> rows=results.all();
		int cont =0;
		cont = (int) rows.get(0).getLong(0);
		return cont; 
	}
	
	@Override
	public int getActivitiesCount(String className) throws SystemException {
		System.out.println("getActivitiesCount3");
		

		BoundStatement boundStatement = new BoundStatement(getActivitiesClassNameIdCountStatement);
		ResultSet results=session.execute(boundStatement.bind(ClassNameLocalServiceUtil.getClassName(className).getClassNameId()));
		List<Row> rows=results.all();
		int cont =0;
		cont = (int) rows.get(0).getLong(0);
		return cont; 	
	}
	@Override
	public SocialActivity getActivity(long activityId) throws PortalException,
			SystemException {
		System.out.println("getActivityNueva");
		BoundStatement boundStatement = new BoundStatement(getActivityStatement);
		ResultSet results=session.execute(boundStatement.bind(activityId));
		if(results!=null )		
		{
			List<Row> rowlist=results.all();
			System.out.println(rowlist.size());
			if(rowlist.size()>0){
				Row row=rowlist.get(0);
				SocialActivity socialActivity = getSocialActivityFromRow(row);
				return socialActivity;
			}
		}
		return null;
	}
	@Override
	public List<SocialActivity> getGroupActivities(long groupId, int start,
			int end) throws SystemException {
		System.out.println("getGroupActivities");
		List<SocialActivity> groupActivities=new java.util.ArrayList<SocialActivity>();
		BoundStatement boundStatement = new BoundStatement(getGroupActivitiesStatement);
		boundStatement.setFetchSize(end);
		ResultSet results=session.execute(boundStatement.bind(groupId,end));
		List<Row> rows=results.all();
		if(rows.size()>0&&rows.size()>=start){
			for(int i=start;i<rows.size();i++){
				Row row=rows.get(i);
				SocialActivity socialActivity = getSocialActivityFromRow(row);
				groupActivities.add(socialActivity);
			}
		}
		return groupActivities;
		
	}
	private SocialActivity getSocialActivityFromRow(Row row)
			throws SystemException {
		SocialActivity socialActivity= this.createSocialActivity(row.getLong("activityid"));
		socialActivity.setClassNameId(row.getLong("classnameid"));
		try {
			socialActivity.setClassName(ClassNameLocalServiceUtil.getClassName(row.getLong("classnameid")).getClassName());
		} catch (PortalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		socialActivity.setClassPK(row.getLong("classpk"));
		socialActivity.setCompanyId(row.getLong("companyid"));
		socialActivity.setCreateDate(row.getDate("createdate").getTime());
		socialActivity.setExtraData(row.getString("extradata"));
		socialActivity.setGroupId(row.getLong("groupid"));
		socialActivity.setMirrorActivityId(row.getLong("mirroractivityid"));
		socialActivity.setReceiverUserId(row.getLong("receiveruserid"));
		socialActivity.setType(row.getInt("type_"));
		socialActivity.setUserId(row.getLong("userid"));
		return socialActivity;
	}
	@Override
	public int getGroupActivitiesCount(long groupId) throws SystemException {
		System.out.println("getGroupActivitiesCount");
		return super.getGroupActivitiesCount(groupId);
	}
	@Override
	public List<SocialActivity> getGroupUsersActivities(long groupId,
			int start, int end) throws SystemException {
		System.out.println("getGroupUsersActivities");

/*			SELECT
				{SocialActivity.*}
			FROM
				SocialActivity
			INNER JOIN
				Users_Groups ON
					(Users_Groups.userId = SocialActivity.userId)
			WHERE
				(SocialActivity.mirrorActivityId = 0) AND
				(Users_Groups.groupId = ?)
			ORDER BY
				SocialActivity.createDate DESC
*/				
		List<User> users =UserLocalServiceUtil.getGroupUsers(groupId);
		List<SocialActivity> groupActivities=new java.util.ArrayList<SocialActivity>();
		BoundStatement boundStatement = new BoundStatement(getGroupUsersActivitiesStatement);
		for(int i=start;i<users.size();i++){
				boundStatement.setFetchSize(end);		
				ResultSet results=session.execute(boundStatement.bind(users.get(i).getUserId(),end));
				List<Row> rows=results.all();
				if(rows.size()>0&&rows.size()>=start){
					for(int j=start;i<rows.size();i++){
						Row row=rows.get(i);
						SocialActivity socialActivity = getSocialActivityFromRow(row);
						groupActivities.add(socialActivity);
					}
				}
		}		
		return groupActivities;
	//	return super.getGroupUsersActivities(groupId, start, end);
	}
	@Override
	public int getGroupUsersActivitiesCount(long groupId)
			throws SystemException {
		System.out.println("getGroupUsersActivitiesCount");
/*
 * 			SELECT
				COUNT(*) AS COUNT_VALUE
			FROM
				SocialActivity
			INNER JOIN
				Users_Groups ON
					(Users_Groups.userId = SocialActivity.userId)
			WHERE
				(SocialActivity.mirrorActivityId = 0) AND
				(Users_Groups.groupId = ?)		
 */
		int cont = 0;
		List<User> users =UserLocalServiceUtil.getGroupUsers(groupId);
		List<SocialActivity> groupActivities=new java.util.ArrayList<SocialActivity>();
		BoundStatement boundStatement = new BoundStatement(getGroupUsersActivitiesCountStatement);
		for(int i=0;i<users.size();i++){
				ResultSet results=session.execute(boundStatement.bind(users.get(i).getUserId()));
				List<Row> rows=results.all();
				if(rows.size()>0&&rows.size()>=0){
					cont = cont + rows.size(); 
				}
		}		
		return cont;		
	}
	@Override
	public SocialActivity getMirrorActivity(long mirrorActivityId)
			throws PortalException, SystemException {
		System.out.println("getMirrorActivity");
		
			BoundStatement boundStatement = new BoundStatement(getMirrorActivityStatement);
			ResultSet results=session.execute(boundStatement.bind(mirrorActivityId));
			if(results!=null ){
				List<Row> rowlist=results.all();
				if(rowlist.size()>0){
					Row row=rowlist.get(0);
					SocialActivity socialActivity = getSocialActivityFromRow(row);
					return socialActivity;
				}
			}
			return null;
		
		//return super.getMirrorActivity(mirrorActivityId);
	}
	@Override
	public List<SocialActivity> getOrganizationActivities(long organizationId,
			int start, int end) throws SystemException {
		System.out.println("getOrganizationActivities");
		
		
		/*
		 * 			SELECT
						{SocialActivity.*}
					FROM
						SocialActivity
					INNER JOIN
						Group_ ON
							(Group_.groupId = SocialActivity.groupId)
					INNER JOIN
						Organization_ ON
							(Organization_.organizationId = Group_.classPK)
					WHERE
						(SocialActivity.mirrorActivityId = 0) AND
						(Organization_.organizationId = ?)
					ORDER BY
						SocialActivity.createDate DESC		
		 */
		

   		List<SocialActivity> groupActivities=new java.util.ArrayList<SocialActivity>();
   		BoundStatement boundStatement = new BoundStatement(getOrganizationActivitiesStatement);
		boundStatement.setFetchSize(end);		
		ResultSet results = null;
		try {
			results = session.execute(boundStatement.bind(OrganizationLocalServiceUtil.getOrganization(organizationId).getGroupId(),end));
		} catch (PortalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   				List<Row> rows=results.all();
   				if(rows.size()>0&&rows.size()>=start){
   					for(int j=start;j<rows.size();j++){
   						Row row=rows.get(j);
   						SocialActivity socialActivity = getSocialActivityFromRow(row);
   						groupActivities.add(socialActivity);
   					}
   		}			
   		
   		return groupActivities;
	//	return super.getOrganizationActivities(organizationId, start, end);
	}
	@Override
	public int getOrganizationActivitiesCount(long organizationId)
			throws SystemException {
		System.out.println("getOrganizationActivitiesCount");
		
		

       int cont =0;
   		BoundStatement boundStatement = new BoundStatement(getOrganizationActivitiesCountStatement);
		ResultSet results = null;
		try {
			results = session.execute(boundStatement.bind(OrganizationLocalServiceUtil.getOrganization(organizationId).getGroupId()));
		} catch (PortalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   				List<Row> rows=results.all();
   				if(rows.size()>0 && rows.size()>=1){
   					cont = (int)  rows.get(0).getLong(0);	   					
   		}			
   		
   		return cont;		
		
		
		//return super.getOrganizationActivitiesCount(organizationId);
	}
	@Override
	public List<SocialActivity> getOrganizationUsersActivities(
			long organizationId, int start, int end) throws SystemException {
		System.out.println("getOrganizationUsersActivities");
/*
 * 			SELECT
				{SocialActivity.*}
			FROM
				SocialActivity
			INNER JOIN
				Users_Orgs ON
					(Users_Orgs.userId = SocialActivity.userId)
			WHERE
				(SocialActivity.mirrorActivityId = 0) AND
				(Users_Orgs.organizationId = ?)
			ORDER BY
				SocialActivity.createDate DESC		
 */
		
		
		List<User> users = UserLocalServiceUtil.getOrganizationUsers(organizationId);
		List<SocialActivity> groupActivities=new java.util.ArrayList<SocialActivity>();
		BoundStatement boundStatement = new BoundStatement(getOrganizationUsersActivitiesStatement);
		for(int i=start;i<users.size();i++){
				boundStatement.setFetchSize(end);		
				ResultSet results=session.execute(boundStatement.bind(users.get(i).getUserId(),end));
				List<Row> rows=results.all();
				if(rows.size()>0&&rows.size()>=start){
					for(int j=start;i<rows.size();i++){
						Row row=rows.get(i);
						SocialActivity socialActivity = getSocialActivityFromRow(row);
						groupActivities.add(socialActivity);
					}
				}
		}			
		
		return groupActivities;
		
		//return super.getOrganizationUsersActivities(organizationId, start, end);
	}
	@Override
	public int getOrganizationUsersActivitiesCount(long organizationId)
			throws SystemException {
		System.out.println("getOrganizationUsersActivitiesCount");
		
        int cont =0;		
		List<User> users = UserLocalServiceUtil.getOrganizationUsers(organizationId);
		BoundStatement boundStatement = new BoundStatement(getOrganizationUsersActivitiesCountStatement);
		for(int i=0;i<users.size();i++){
				ResultSet results=session.execute(boundStatement.bind(users.get(i).getUserId()));
				List<Row> rows=results.all();
				cont = cont + (int) rows.get(0).getLong(0);				
		}			
		
		return cont;
		
		
		//return super.getOrganizationUsersActivitiesCount(organizationId);
	}
	
/******RELACTION******************************************************************************************/	
	@Override
	public List<SocialActivity> getRelationActivities(long userId, int start,
			int end) throws SystemException {
		System.out.println("getRelationActivities");
		return super.getRelationActivities(userId, start, end);
	}
	@Override
	public List<SocialActivity> getRelationActivities(long userId, int type,
			int start, int end) throws SystemException {
		System.out.println("getRelationActivities2");
		return super.getRelationActivities(userId, type, start, end);
	}
	@Override
	public int getRelationActivitiesCount(long userId) throws SystemException {
		System.out.println("getRelationActivitiesCount");
		return super.getRelationActivitiesCount(userId);
	}
	@Override
	public int getRelationActivitiesCount(long userId, int type)
			throws SystemException {
		System.out.println("getRelationActivitiesCount2");
		return super.getRelationActivitiesCount(userId, type);
	}
	
/************************************************************************************************/	
	
	@Override
	public List<SocialActivity> getUserActivities(long userId, int start,
			int end) throws SystemException {
		System.out.println("getUserActivities");
		
		List<SocialActivity> groupActivities=new java.util.ArrayList<SocialActivity>();
		BoundStatement boundStatement = new BoundStatement(getActivitiesLimitStatement);
		boundStatement.setFetchSize(end);
		ResultSet results=session.execute(boundStatement.bind(userId,end));
		List<Row> rows=results.all();
		if(rows.size()>0&&rows.size()>=start){
			for(int i=start;i<rows.size();i++){
				Row row=rows.get(i);
				SocialActivity socialActivity = getSocialActivityFromRow(row);
				groupActivities.add(socialActivity);
			}
		}
		return groupActivities;
		
	}
	@Override
	public int getUserActivitiesCount(long userId) throws SystemException {
		System.out.println("getUserActivitiesCount");
		
		
		BoundStatement boundStatement = new BoundStatement(getActivitiesCountStatement);
		ResultSet results=session.execute(boundStatement.bind(userId));
		List<Row> rows=results.all();
		int cont =0;
		cont = (int) rows.get(0).getLong(0);
		return cont; 
		
	}
	@Override
	public List<SocialActivity> getUserGroupsActivities(long userId, int start,
			int end) throws SystemException {
		System.out.println("getUserGroupsActivities");
		System.out.println("NO IMPLEMENTADO");
		
	//	UserLocalServiceUtil.get
		
/*
 * 		SELECT
				{SocialActivity.*}
			FROM
				SocialActivity
			WHERE
				(
					groupId IN (
						SELECT
							groupId
						FROM
							Users_Groups
						WHERE
							userId = ?
					)
				) AND
				(mirrorActivityId = 0)
			ORDER BY
				createDate DESC
		]]>		
 */
		
		return super.getUserGroupsActivities(userId, start, end);
	}
	@Override
	public int getUserGroupsActivitiesCount(long userId) throws SystemException {
		System.out.println("getUserGroupsActivitiesCount");
		System.out.println("NO IMPLEMENTADO");		
		return super.getUserGroupsActivitiesCount(userId);
	}
	@Override
	public List<SocialActivity> getUserGroupsAndOrganizationsActivities(
			long userId, int start, int end) throws SystemException {
		System.out.println("getUserGroupsAndOrganizationsActivities");
		System.out.println("NO IMPLEMENTADO");
		return super.getUserGroupsAndOrganizationsActivities(userId, start, end);
	}
	@Override
	public int getUserGroupsAndOrganizationsActivitiesCount(long userId)
			throws SystemException {
		System.out.println("getUserGroupsAndOrganizationsActivitiesCount");
		System.out.println("NO IMPLEMENTADO");		
		return super.getUserGroupsAndOrganizationsActivitiesCount(userId);
	}
	@Override
	public List<SocialActivity> getUserOrganizationsActivities(long userId,
			int start, int end) throws SystemException {
		System.out.println("getUserOrganizationsActivities");
		System.out.println("NO IMPLEMENTADO");		
		return super.getUserOrganizationsActivities(userId, start, end);
	}
	@Override
	public int getUserOrganizationsActivitiesCount(long userId)
			throws SystemException {
		System.out.println("getUserOrganizationsActivitiesCount");
		
		System.out.println("NO IMPLEMENTADO");
		
/*
 * 			SELECT
				COUNT(*) AS COUNT_VALUE
			FROM
				SocialActivity
			INNER JOIN
				Group_ ON
					(Group_.groupId = SocialActivity.groupId)
			INNER JOIN
				Organization_ ON
					(Organization_.organizationId = Group_.classPK)
			WHERE
				(SocialActivity.mirrorActivityId = 0) AND
				(
					Organization_.organizationId IN (
						SELECT
							organizationId
						FROM
							Users_Orgs
						WHERE
							Users_Orgs.userId = ?
					)
				)		
 */
		
		return super.getUserOrganizationsActivitiesCount(userId);
	}

}