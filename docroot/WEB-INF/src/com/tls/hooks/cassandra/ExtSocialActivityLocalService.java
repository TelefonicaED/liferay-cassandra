package com.tls.hooks.cassandra;

import java.util.Date;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.liferay.portal.kernel.exception.PortalException;
import com.liferay.portal.kernel.exception.SystemException;
import com.liferay.portlet.asset.model.AssetEntry;
import com.liferay.portlet.social.model.SocialActivity;
import com.liferay.portlet.social.service.SocialActivityLocalServiceWrapper;
import com.liferay.portlet.social.service.SocialActivityLocalService;

public class ExtSocialActivityLocalService extends SocialActivityLocalServiceWrapper {
	/* (non-Java-doc)
	 * @see com.liferay.portlet.social.service.SocialActivityLocalServiceWrapper#SocialActivityLocalServiceWrapper(SocialActivityLocalService socialActivityLocalService)
	 */
	private static Cluster cluster;
	private static Session session;
	static String node="10.102.227.51";

	   public Session getSession() 
	   {
	      return this.session;
	   }
	   public void createSchema() {
		      session.execute("CREATE KEYSPACE IF NOT EXISTS liferay WITH replication " + 
		            "= {'class':'SimpleStrategy', 'replication_factor':1};");
		      session.execute(
		            "CREATE TABLE IF NOT EXISTS liferay.socialactivity (" +
		                  "activityId bigint PRIMARY KEY," + 
		                  "groupId bigint," + 
		                  "companyId bigint," + 
		                  "userId bigint," + 
		                  "createDate timestamp," +
		                  "mirrorActivityId bigint," +
		                  "classNameId bigint," +
		                  "classPK bigint," +
		                  "extraData varchar," +
		                  "receiverUserId bigint" + 
		                  ");");
		      
		   }
	   public void connect() 
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
	      cluster.close();
		   
	   }
	public ExtSocialActivityLocalService(SocialActivityLocalService socialActivityLocalService) {
		super(socialActivityLocalService);
	}
	@Override
	public SocialActivity addSocialActivity(SocialActivity socialActivity)
			throws SystemException {
		// TODO Auto-generated method stub
		System.out.println("sa");
		connect();
		return super.addSocialActivity(socialActivity);
		
	}
	@Override
	public SocialActivity createSocialActivity(long activityId) {
		// TODO Auto-generated method stub
		connect();
		return super.createSocialActivity(activityId);
	}
	@Override
	public SocialActivity deleteSocialActivity(long activityId)
			throws PortalException, SystemException {
		// TODO Auto-generated method stub
		connect();
		return super.deleteSocialActivity(activityId);
	}
	@Override
	public SocialActivity deleteSocialActivity(SocialActivity socialActivity)
			throws SystemException {
		// TODO Auto-generated method stub
		connect();
		return super.deleteSocialActivity(socialActivity);
	}
	@Override
	public SocialActivity fetchSocialActivity(long activityId)
			throws SystemException {
		// TODO Auto-generated method stub
		connect();
		return super.fetchSocialActivity(activityId);
	}
	@Override
	public SocialActivity getSocialActivity(long activityId)
			throws PortalException, SystemException {
		// TODO Auto-generated method stub
		connect();
		return super.getSocialActivity(activityId);
	}
	@Override
	public List<SocialActivity> getSocialActivities(int start, int end)
			throws SystemException {
		// TODO Auto-generated method stub
		connect();
		return super.getSocialActivities(start, end);
	}
	@Override
	public int getSocialActivitiesCount() throws SystemException {
		// TODO Auto-generated method stub
		connect();
		return super.getSocialActivitiesCount();
	}
	@Override
	public SocialActivity updateSocialActivity(SocialActivity socialActivity)
			throws SystemException {
		// TODO Auto-generated method stub
		return super.updateSocialActivity(socialActivity);
	}
	@Override
	public SocialActivity updateSocialActivity(SocialActivity socialActivity,
			boolean merge) throws SystemException {
		// TODO Auto-generated method stub
		connect();
		return super.updateSocialActivity(socialActivity, merge);
	}
	@Override
	public void addActivity(long userId, long groupId, Date createDate,
			String className, long classPK, int type, String extraData,
			long receiverUserId) throws PortalException, SystemException {
		// TODO Auto-generated method stub
		connect();
		super.addActivity(userId, groupId, createDate, className, classPK, type,
				extraData, receiverUserId);
	}
	@Override
	public void addActivity(long userId, long groupId, String className,
			long classPK, int type, String extraData, long receiverUserId)
			throws PortalException, SystemException {
		// TODO Auto-generated method stub
		connect();
		super.addActivity(userId, groupId, className, classPK, type, extraData,
				receiverUserId);
	}
	@Override
	public void addActivity(SocialActivity activity,
			SocialActivity mirrorActivity) throws PortalException,
			SystemException {
		System.out.println("sasa");
		// TODO Auto-generated method stub
		connect();
		super.addActivity(activity, mirrorActivity);
	}
	@Override
	public void addUniqueActivity(long userId, long groupId, Date createDate,
			String className, long classPK, int type, String extraData,
			long receiverUserId) throws PortalException, SystemException {
		// TODO Auto-generated method stub
		connect();
		super.addUniqueActivity(userId, groupId, createDate, className, classPK, type,
				extraData, receiverUserId);
	}
	@Override
	public void addUniqueActivity(long userId, long groupId, String className,
			long classPK, int type, String extraData, long receiverUserId)
			throws PortalException, SystemException {
		// TODO Auto-generated method stub
		connect();
		super.addUniqueActivity(userId, groupId, className, classPK, type, extraData,
				receiverUserId);
	}
	@Override
	public void deleteActivities(AssetEntry assetEntry) throws PortalException,
			SystemException {
		// TODO Auto-generated method stub
		connect();
		super.deleteActivities(assetEntry);
	}
	@Override
	public void deleteActivities(String className, long classPK)
			throws SystemException {
		// TODO Auto-generated method stub
		connect();
		super.deleteActivities(className, classPK);
	}
	@Override
	public void deleteActivity(long activityId) throws PortalException,
			SystemException {
		// TODO Auto-generated method stub
		connect();
		super.deleteActivity(activityId);
	}
	@Override
	public void deleteActivity(SocialActivity activity) throws SystemException {
		// TODO Auto-generated method stub
		connect();
		super.deleteActivity(activity);
	}
	@Override
	public void deleteUserActivities(long userId) throws SystemException {
		// TODO Auto-generated method stub
		connect();
		super.deleteUserActivities(userId);
	}
	@Override
	public List<SocialActivity> getActivities(long classNameId, int start,
			int end) throws SystemException {
		// TODO Auto-generated method stub
		connect();
		return super.getActivities(classNameId, start, end);
	}
	@Override
	public List<SocialActivity> getActivities(long mirrorActivityId,
			long classNameId, long classPK, int start, int end)
			throws SystemException {
		// TODO Auto-generated method stub
		connect();
		return super.getActivities(mirrorActivityId, classNameId, classPK, start, end);
	}
	@Override
	public List<SocialActivity> getActivities(long mirrorActivityId,
			String className, long classPK, int start, int end)
			throws SystemException {
		// TODO Auto-generated method stub
		connect();
		return super.getActivities(mirrorActivityId, className, classPK, start, end);
	}
	@Override
	public List<SocialActivity> getActivities(String className, int start,
			int end) throws SystemException {
		// TODO Auto-generated method stub
		connect();
		return super.getActivities(className, start, end);
	}
	@Override
	public int getActivitiesCount(long classNameId) throws SystemException {
		// TODO Auto-generated method stub
		connect();
		return super.getActivitiesCount(classNameId);
	}
	@Override
	public int getActivitiesCount(long mirrorActivityId, long classNameId,
			long classPK) throws SystemException {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		return super.getActivitiesCount(className);
	}
	@Override
	public SocialActivity getActivity(long activityId) throws PortalException,
			SystemException {
		// TODO Auto-generated method stub
		return super.getActivity(activityId);
	}
	@Override
	public List<SocialActivity> getGroupActivities(long groupId, int start,
			int end) throws SystemException {
		// TODO Auto-generated method stub
		return super.getGroupActivities(groupId, start, end);
	}
	@Override
	public int getGroupActivitiesCount(long groupId) throws SystemException {
		// TODO Auto-generated method stub
		return super.getGroupActivitiesCount(groupId);
	}
	@Override
	public List<SocialActivity> getGroupUsersActivities(long groupId,
			int start, int end) throws SystemException {
		// TODO Auto-generated method stub
		return super.getGroupUsersActivities(groupId, start, end);
	}
	@Override
	public int getGroupUsersActivitiesCount(long groupId)
			throws SystemException {
		// TODO Auto-generated method stub
		return super.getGroupUsersActivitiesCount(groupId);
	}
	@Override
	public SocialActivity getMirrorActivity(long mirrorActivityId)
			throws PortalException, SystemException {
		// TODO Auto-generated method stub
		return super.getMirrorActivity(mirrorActivityId);
	}
	@Override
	public List<SocialActivity> getOrganizationActivities(long organizationId,
			int start, int end) throws SystemException {
		// TODO Auto-generated method stub
		return super.getOrganizationActivities(organizationId, start, end);
	}
	@Override
	public int getOrganizationActivitiesCount(long organizationId)
			throws SystemException {
		// TODO Auto-generated method stub
		return super.getOrganizationActivitiesCount(organizationId);
	}
	@Override
	public List<SocialActivity> getOrganizationUsersActivities(
			long organizationId, int start, int end) throws SystemException {
		// TODO Auto-generated method stub
		return super.getOrganizationUsersActivities(organizationId, start, end);
	}
	@Override
	public int getOrganizationUsersActivitiesCount(long organizationId)
			throws SystemException {
		// TODO Auto-generated method stub
		return super.getOrganizationUsersActivitiesCount(organizationId);
	}
	@Override
	public List<SocialActivity> getRelationActivities(long userId, int start,
			int end) throws SystemException {
		// TODO Auto-generated method stub
		return super.getRelationActivities(userId, start, end);
	}
	@Override
	public List<SocialActivity> getRelationActivities(long userId, int type,
			int start, int end) throws SystemException {
		// TODO Auto-generated method stub
		return super.getRelationActivities(userId, type, start, end);
	}
	@Override
	public int getRelationActivitiesCount(long userId) throws SystemException {
		// TODO Auto-generated method stub
		return super.getRelationActivitiesCount(userId);
	}
	@Override
	public int getRelationActivitiesCount(long userId, int type)
			throws SystemException {
		// TODO Auto-generated method stub
		return super.getRelationActivitiesCount(userId, type);
	}
	@Override
	public List<SocialActivity> getUserActivities(long userId, int start,
			int end) throws SystemException {
		// TODO Auto-generated method stub
		return super.getUserActivities(userId, start, end);
	}
	@Override
	public int getUserActivitiesCount(long userId) throws SystemException {
		// TODO Auto-generated method stub
		return super.getUserActivitiesCount(userId);
	}
	@Override
	public List<SocialActivity> getUserGroupsActivities(long userId, int start,
			int end) throws SystemException {
		// TODO Auto-generated method stub
		return super.getUserGroupsActivities(userId, start, end);
	}
	@Override
	public int getUserGroupsActivitiesCount(long userId) throws SystemException {
		// TODO Auto-generated method stub
		return super.getUserGroupsActivitiesCount(userId);
	}
	@Override
	public List<SocialActivity> getUserGroupsAndOrganizationsActivities(
			long userId, int start, int end) throws SystemException {
		// TODO Auto-generated method stub
		return super.getUserGroupsAndOrganizationsActivities(userId, start, end);
	}
	@Override
	public int getUserGroupsAndOrganizationsActivitiesCount(long userId)
			throws SystemException {
		// TODO Auto-generated method stub
		return super.getUserGroupsAndOrganizationsActivitiesCount(userId);
	}
	@Override
	public List<SocialActivity> getUserOrganizationsActivities(long userId,
			int start, int end) throws SystemException {
		// TODO Auto-generated method stub
		return super.getUserOrganizationsActivities(userId, start, end);
	}
	@Override
	public int getUserOrganizationsActivitiesCount(long userId)
			throws SystemException {
		// TODO Auto-generated method stub
		return super.getUserOrganizationsActivitiesCount(userId);
	}

}