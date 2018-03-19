package com.thomsonreuters.gcp.feedPrc4Trmi;

import com.thomsonreuters.ema.access.Msg;
import com.thomsonreuters.ema.access.OmmArray;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.thomsonreuters.ema.access.AckMsg;
import com.thomsonreuters.ema.access.ElementList;
import com.thomsonreuters.ema.access.GenericMsg;
import com.thomsonreuters.ema.access.RefreshMsg;
import com.thomsonreuters.ema.access.ReqMsg;
import com.thomsonreuters.ema.access.StatusMsg;
import com.thomsonreuters.ema.access.UpdateMsg;
import com.thomsonreuters.ema.rdm.EmaRdm;
import com.thomsonreuters.ema.access.EmaFactory;
import com.thomsonreuters.ema.access.OmmConsumer;
import com.thomsonreuters.ema.access.OmmConsumerClient;
import com.thomsonreuters.ema.access.OmmConsumerConfig;
import com.thomsonreuters.ema.access.OmmConsumerEvent;
import com.thomsonreuters.ema.access.OmmException;

import com.thomsonreuters.gcp.feedPrc4Trmi.emaAppClient;


public class emaConnectionManagement extends connectionManagementAbs {
	String _ipAddress;
	int _portNumber;
	OmmConsumer _consumer;
	
	// Additional Parameters for EMS
	String _userName;
	String _serverInstanceName;
	String _inSymbol;
	String _inFID;
	
	Map<String,String> fidMap;
	
	emaConnectionManagement()
	{
		_consumer = null;
	}
	
	public void setBasicConnectionInfo(String ipAddr, int numPort)
	{
		_ipAddress = ipAddr;
		_portNumber = numPort;
	}
	
	public void setAdditionalConnectionInfo(String otherParams)
	{
		// userName:serverInstanceName are required as input parameters
		String[] __inputParams = otherParams.split(":",0);
		_userName = __inputParams[0];
		_serverInstanceName = __inputParams[1];
		_inSymbol = __inputParams[2];
		_inFID = __inputParams[3];
	}
	
	@SuppressWarnings("finally")
	public boolean connectionInitiation()
	{
		boolean _ret = true;
		// OmmConsumer Instantiation
		_consumer = null;
		try{
			// emaAppClient appClient = new emaAppClient();
			// Count number of symbols in name file to setup instances of appClient 
			// @SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(new FileReader(_inSymbol));
			int count = 1;
			while(true)
			{
				String line = br.readLine();
				if (line == null)
					break;
				count ++;
			}
			br.close();
			
			// emaAppClient appClient = new emaAppClient();
			// load FID key and fieldnames list into hashMap
			// @SuppressWarnings("resource")
			fidMap = new HashMap<String,String>();
			BufferedReader fbr = new BufferedReader(new FileReader(_inFID));
			while(true)
			{
				String line = fbr.readLine();
				if (line == null)
					break;
				String[] fids = line.split(",");
				fidMap.put(fids[0], fids[1]);
			}
			
			emaAppClient appClient[] = new emaAppClient[count];
			for (int i = 0; i < count; i ++)
				appClient[i] = new emaAppClient();
			
			OmmConsumerConfig config = EmaFactory.createOmmConsumerConfig();
			String ipAndPort = _ipAddress + ":" + String.valueOf(_portNumber);
			_consumer = EmaFactory.createOmmConsumer(config.host(ipAndPort).username(_userName));
			ReqMsg reqMsg = EmaFactory.createReqMsg();

			//ElementList batch = EmaFactory.createElementList();
			//OmmArray array = EmaFactory.createOmmArray();
			//@SuppressWarnings("resource")
			BufferedReader br1 = new BufferedReader(new FileReader(_inSymbol));	
			int numRecord = 0;
			while(true)
			{
				String line = br1.readLine();
				if (line == null)
					break;
				String[] sym = line.split(",");
				String fieldparams = fidMap.get(sym[1]);
				System.out.print(sym[0] + ":" );
				appClient[numRecord].setupTargetField(fieldparams);
				_consumer.registerClient(reqMsg.serviceName(_serverInstanceName).name(sym[0]), appClient[numRecord]);
				numRecord ++;
				//array.add(EmaFactory.createOmmArrayEntry().ascii(line));
			}
			br1.close();
			//batch.add(EmaFactory.createElementEntry().array(EmaRdm.ENAME_BATCH_ITEM_LIST, array));
			//_consumer.registerClient(reqMsg.serviceName(_serverInstanceName).payload(batch), appClient);

			
			while(true)
				Thread.sleep(600000);

		}
		catch(InterruptedException | OmmException | IOException excp )
		{
			System.out.println(excp.getMessage());
			_ret = false;
			return _ret;
		} 
		
		
		finally
		{
			if (_consumer != null)
				_consumer.uninitialize();
			return _ret;
		}
		
	}
	

	
}
