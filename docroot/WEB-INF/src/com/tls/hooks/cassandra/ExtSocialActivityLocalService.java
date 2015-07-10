package com.tls.hooks.cassandra;

import java.util.Date;
import java.util.List;

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
import com.liferay.portal.model.Layout;
import com.liferay.portal.model.User;
import com.liferay.portal.service.ClassNameLocalServiceUtil;
import com.liferay.portal.service.GroupLocalServiceUtil;
import com.liferay.portal.service.LayoutLocalServiceUtil;
import com.liferay.portal.service.UserGroupLocalServiceUtil;
import com.liferay.portal.service.UserLocalServiceUtil;
import com.liferay.portal.util.PortalUtil;
import com.liferay.portlet.asset.model.AssetEntry;
import com.liferay.portlet.asset.service.AssetEntryLocalServiceUtil;
import com.liferay.portlet.social.model.SocialActivity;
import com.liferay.portlet.social.model.SocialActivityDefinition;
import com.liferay.portlet.social.model.SocialRequestWrapper;
import com.liferay.portlet.social.service.SocialActivityCounterLocalServiceUtil;
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
	PreparedStatement getActivitiesStament;
	PreparedStatement getGroupUsersActivitiesStament;
	PreparedStatement getGroupUsersActivitiesCountStament;	
	
      	
	
	
	
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
			 getActivitiesStament = session.prepare(
				      "select * from liferay.socialactivity where userId= ? limit ?;");
			 getGroupUsersActivitiesStament = session.prepare(
				      "select * from liferay.socialactivity where userId= ? and mirrorActivityId=0 limit ?  ALLOW FILTERING;");
			 getGroupUsersActivitiesCountStament = session.prepare(
				      "select count(*) from liferay.socialactivity where userId= ? and mirrorActivityId=0 limit ?  ALLOW FILTERING;");
			 
			 //DELETE
			 
		      
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
		return super.deleteSocialActivity(activityId);
	}
	@Override
	public SocialActivity deleteSocialActivity(SocialActivity socialActivity)
			throws SystemException {
		System.out.println("deleteSocialActivity");
		return super.deleteSocialActivity(socialActivity);
	}
	@Override
	public SocialActivity fetchSocialActivity(long activityId)
			throws SystemException {
		System.out.println("fetchSocialActivity");
		return super.fetchSocialActivity(activityId);
	}
	@Override
	public SocialActivity getSocialActivity(long activityId)
			throws PortalException, SystemException {
		System.out.println("getSocialActivity");
		return this.getActivity(activityId);
	}
	@Override
	public List<SocialActivity> getSocialActivities(int start, int end)
			throws SystemException {
		System.out.println("getSocialActivities");
		return super.getSocialActivities(start, end);
	}
	@Override
	public int getSocialActivitiesCount() throws SystemException {
		System.out.println("getSocialActivitiesCount");
		return super.getSocialActivitiesCount();
	}
	@Override
	public SocialActivity updateSocialActivity(SocialActivity socialActivity)
			throws SystemException {
		System.out.println("updateSocialActivity");
		return super.updateSocialActivity(socialActivity);
	}
	@Override
	public SocialActivity updateSocialActivity(SocialActivity socialActivity,
			boolean merge) throws SystemException {
		System.out.println("updateSocialActivity");
		return super.updateSocialActivity(socialActivity, merge);
	}
	@Override
	public void addActivity(long userId, long groupId, Date createDate,
			String className, long classPK, int type, String extraData,
			long receiverUserId) throws PortalException, SystemException {
		System.out.println("addActivity1");
		
		
		
//CASSANDRA
		
		int cont = 0;
		List<User> users =UserLocalServiceUtil.getGroupUsers(groupId);
		BoundStatement boundStatement = new BoundStatement(getGroupUsersActivitiesCountStament);
		if (users.size()>0){
			for(int i=1;i<users.size();i++){
					ResultSet results=session.execute(boundStatement.bind(users.get(i).getUserId()));
					List<Row> rows=results.all();
					if(rows.size()>0&&rows.size()>=0){
						cont = cont + rows.size(); 
					}
			}		
		}
		
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
		System.out.println("deleteActivities");
		
		super.deleteActivities(assetEntry);
	}
	@Override
	public void deleteActivities(String className, long classPK)
			throws SystemException {
		System.out.println("deleteActivities");
		
		super.deleteActivities(className, classPK);
	}
	@Override
	public void deleteActivity(long activityId) throws PortalException,
			SystemException {
		System.out.println("deleteActivity");
		
		super.deleteActivity(activityId);
	}
	@Override
	public void deleteActivity(SocialActivity activity) throws SystemException {
		// TODO Auto-generated method stub
		System.out.println("deleteActivity");
		super.deleteActivity(activity);
	}
	@Override
	public void deleteUserActivities(long userId) throws SystemException {
		System.out.println("deleteUserActivities");
		
		super.deleteUserActivities(userId);
	}
	@Override
	public List<SocialActivity> getActivities(long classNameId, int start,
			int end) throws SystemException {
		System.out.println("getActivities1");
		return super.getActivities(classNameId, start, end);
	}
	@Override
	public List<SocialActivity> getActivities(long mirrorActivityId,
			long classNameId, long classPK, int start, int end)
			throws SystemException {
		System.out.println("getActivities2");
		return super.getActivities(mirrorActivityId, classNameId, classPK, start, end);
	}
	@Override
	public List<SocialActivity> getActivities(long mirrorActivityId,
			String className, long classPK, int start, int end)
			throws SystemException {
		System.out.println("getActivities3");
		
		return super.getActivities(mirrorActivityId, className, classPK, start, end);
	}
	@Override
	public List<SocialActivity> getActivities(String className, int start,
			int end) throws SystemException {
		System.out.println("getActivities4");
		
		return super.getActivities(className, start, end);
	}
	@Override
	public int getActivitiesCount(long classNameId) throws SystemException {
		System.out.println("getActivitiesCount");
		
		return super.getActivitiesCount(classNameId);
	}
	@Override
	public int getActivitiesCount(long mirrorActivityId, long classNameId,
			long classPK) throws SystemException {
		System.out.println("getActivitiesCount2");
		
		return super.getActivitiesCount(mirrorActivityId, classNameId, classPK);
	}
	@Override
	public int getActivitiesCount(long mirrorActivityId, String className,
			long classPK) throws SystemException {
		// TODO Auto-generated method stub
		return super.getActivitiesCount(mirrorActivityId, className, classPK);
	}
	@Override
	public int getActivitiesCount(String className) throws SystemException {
		System.out.println("getActivitiesCount3");
		
		return super.getActivitiesCount(className);
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
		BoundStatement boundStatement = new BoundStatement(getGroupUsersActivitiesStament);
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
		BoundStatement boundStatement = new BoundStatement(getGroupUsersActivitiesCountStament);
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
		return super.getMirrorActivity(mirrorActivityId);
	}
	@Override
	public List<SocialActivity> getOrganizationActivities(long organizationId,
			int start, int end) throws SystemException {
		System.out.println("getOrganizationActivities");
		return super.getOrganizationActivities(organizationId, start, end);
	}
	@Override
	public int getOrganizationActivitiesCount(long organizationId)
			throws SystemException {
		System.out.println("getOrganizationActivitiesCount");
		return super.getOrganizationActivitiesCount(organizationId);
	}
	@Override
	public List<SocialActivity> getOrganizationUsersActivities(
			long organizationId, int start, int end) throws SystemException {
		System.out.println("getOrganizationUsersActivities");
		return super.getOrganizationUsersActivities(organizationId, start, end);
	}
	@Override
	public int getOrganizationUsersActivitiesCount(long organizationId)
			throws SystemException {
		System.out.println("getOrganizationUsersActivitiesCount");
		return super.getOrganizationUsersActivitiesCount(organizationId);
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
		BoundStatement boundStatement = new BoundStatement(getActivitiesStament);
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
		
		return super.getUserActivitiesCount(userId);
	}
	@Override
	public List<SocialActivity> getUserGroupsActivities(long userId, int start,
			int end) throws SystemException {
		System.out.println("getUserGroupsActivities");
		
		return super.getUserGroupsActivities(userId, start, end);
	}
	@Override
	public int getUserGroupsActivitiesCount(long userId) throws SystemException {
		System.out.println("getUserGroupsActivitiesCount");
		return super.getUserGroupsActivitiesCount(userId);
	}
	@Override
	public List<SocialActivity> getUserGroupsAndOrganizationsActivities(
			long userId, int start, int end) throws SystemException {
		System.out.println("getUserGroupsAndOrganizationsActivities");
		return super.getUserGroupsAndOrganizationsActivities(userId, start, end);
	}
	@Override
	public int getUserGroupsAndOrganizationsActivitiesCount(long userId)
			throws SystemException {
		System.out.println("getUserGroupsAndOrganizationsActivitiesCount");
		return super.getUserGroupsAndOrganizationsActivitiesCount(userId);
	}
	@Override
	public List<SocialActivity> getUserOrganizationsActivities(long userId,
			int start, int end) throws SystemException {
		System.out.println("getUserOrganizationsActivities");
		return super.getUserOrganizationsActivities(userId, start, end);
	}
	@Override
	public int getUserOrganizationsActivitiesCount(long userId)
			throws SystemException {
		System.out.println("getUserOrganizationsActivitiesCount");
		
		return super.getUserOrganizationsActivitiesCount(userId);
	}

}