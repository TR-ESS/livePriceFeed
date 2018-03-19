package com.thomsonreuters.gcp.feedPrc4Trmi;

import com.thomsonreuters.gcp.feedPrc4Trmi.emaConnectionManagement;
import org.apache.commons.cli.*;

import javax.xml.parsers.*;

public class emaMain {

	public static void main(String[] args)  {
		// TODO Auto-generated method stub
		
		// External Parameters Parse
		// -h: hostname [String]
		// -p: port number [int]
		// -u: user name [String]
		// -s: server instance name [String]
		
		Options options = new Options();
		Option hostName = new Option("h", "hostname", true, "input host name");
		hostName.setRequired(true);
		options.addOption(hostName);
		
		Option portNumber = new Option("p", "portnumber",true, "input port number");
		portNumber.setRequired(true);
		portNumber.setType(Number.class);
		options.addOption(portNumber);
		
		Option userName = new Option("u","user",true,"input user name");
		userName.setRequired(true);
		options.addOption(userName);
		
		Option serverInstance = new Option("s","server",true,"input server instance name");
		serverInstance.setRequired(true);
		options.addOption(serverInstance);

		Option inFile = new Option("n","name",true,"RIC name");
		inFile.setRequired(true);
		options.addOption(inFile);
		
		Option fidFile = new Option("f","fid", true, "FID name");
		fidFile.setRequired(true);
		options.addOption(fidFile);
		
		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd;
		try{
			cmd = parser.parse(options, args);
		} catch (ParseException e)
		{
			System.out.println(e.getMessage());
			formatter.printHelp("utility-name", options);
			System.exit(1);
			return;
		}
		
		String inHostName = cmd.getOptionValue("hostname");
		int inPortNumber = Integer.valueOf(cmd.getOptionValue("portnumber"));
		String inUser = cmd.getOptionValue("user");
		String inServerInstance = cmd.getOptionValue("server");
		String inSymbol = cmd.getOptionValue("name");
		String inFID = cmd.getOptionValue("fid");
		
		emaConnectionManagement conn = new emaConnectionManagement();
		// LOAD CONNECTION INFO FROM emaConnectionManagement.xml
		
		// conn.setBasicConnectionInfo("10.33.110.129", 14002);
		conn.setBasicConnectionInfo(inHostName, inPortNumber);
		conn.setAdditionalConnectionInfo(inUser + ":" + inServerInstance + ":" + inSymbol + ":" + inFID);
		
		boolean ret = conn.connectionInitiation();
		if (!ret)
			System.out.println("FAILURE in Main");

	}

}
