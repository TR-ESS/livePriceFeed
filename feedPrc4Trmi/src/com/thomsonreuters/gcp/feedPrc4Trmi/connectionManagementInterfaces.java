package com.thomsonreuters.gcp.feedPrc4Trmi;

public interface connectionManagementInterfaces {
	void setBasicConnectionInfo(String ipaddr, int portNumber);
	boolean connectionInitiation();
	void setAdditionalConnectionInfo(String addditionalArgs);
}
