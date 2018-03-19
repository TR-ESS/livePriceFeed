package com.thomsonreuters.gcp.feedPrc4Trmi;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.DateUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;

import com.thomsonreuters.ema.access.AckMsg;
import com.thomsonreuters.ema.access.DataType.DataTypes;
import com.thomsonreuters.ema.access.FieldEntry;
import com.thomsonreuters.ema.access.FieldList;
import com.thomsonreuters.ema.access.GenericMsg;
import com.thomsonreuters.ema.access.Msg;
import com.thomsonreuters.ema.access.OmmConsumerClient;
import com.thomsonreuters.ema.access.OmmConsumerEvent;
import com.thomsonreuters.ema.access.RefreshMsg;
import com.thomsonreuters.ema.access.StatusMsg;
import com.thomsonreuters.ema.access.UpdateMsg;

public class emaAppClient implements OmmConsumerClient {
	double _bidPrice;
	double _askPrice;
	double _midPrice;
	
	double _openPrice;
	double _highPrice;
	double _lowPrice;
	double _lastPrice;
	
	String trPriceFieldName;
	String trActiveDateFieldName;
	String trTimeFieldName;
	
	String thisActiveDate;
	
	String _prevHour;
	
	emaAppClient()
	{
		_openPrice = 0;
		_highPrice = -99999;
		_lowPrice = 99999;
		_lastPrice = 0;
		_prevHour = "99";
		thisActiveDate = "";
	}
	
	public void setupTargetField(String arg)
	{
		String[] _targetFields = arg.split(":");
		trPriceFieldName = _targetFields[0];
		trActiveDateFieldName = _targetFields[1];
		trTimeFieldName = _targetFields[2];
		System.out.println(trPriceFieldName);
	}
	
	@Override
	public void onAckMsg(AckMsg ackMsg, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onAllMsg(Msg msg, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onGenericMsg(GenericMsg genMsg, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub
		System.out.println("GenericMsg: " + genMsg);
	}

	@Override
	public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub
		if (refreshMsg.payload().dataType() != DataTypes.NO_DATA)
		{
			msgDecode(refreshMsg.payload().fieldList(),refreshMsg.name().toString());
		}
	}

	@Override
	public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub
		System.out.println("OnStatusMsg: " + statusMsg);
		
	}

	@Override
	public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub
		if (updateMsg.payload().dataType() != DataTypes.NO_DATA)
		{	
			msgDecode(updateMsg.payload().fieldList(), updateMsg.name().toString());
		}
		
	}
	
	private void msgDecode(FieldList fieldList, String assetCode)
	{
		Iterator<FieldEntry> iter = fieldList.iterator();
		FieldEntry fieldEntry;
		String _activeDate = "";
		String _quoteTime = "";
		int _updateFlag = 0;
		String _currHour = "";
		
		while(iter.hasNext())
		{
			fieldEntry = iter.next();
			String fieldName = fieldEntry.name();

			if (fieldName.equals(trPriceFieldName))
			{
				//System.out.println(assetCode + ":FieldID=" + fieldEntry.fieldId() + "FieldName=" + fieldEntry.name() + "," + "Value=" + fieldEntry.load());
				if (fieldEntry.load().toString().equals(""))
					break;
				try{
					_bidPrice = Double.parseDouble(fieldEntry.load().toString());
				}
				catch(Exception e){
					System.out.println("Object exception: " + e.getMessage());
					break;
				}
				_updateFlag++;

			}
			
			if (fieldName.equals(trActiveDateFieldName))
			{
				//System.out.println(assetCode + ":FieldID=" + fieldEntry.fieldId() + "FieldName=" + fieldEntry.name() + "," + "Value=" + fieldEntry.load());
				_activeDate = fieldEntry.load().toString();
				if (!_activeDate.equals(""))
				{
					SimpleDateFormat _sDate = new SimpleDateFormat("dd MMM yyyy", Locale.ENGLISH);
					SimpleDateFormat _sDate2 = new SimpleDateFormat("yyyy-MM-dd");
					try {
						Date _date = _sDate.parse(_activeDate);
						_activeDate = _sDate2.format(_date);
						thisActiveDate = _activeDate;
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
			}
			if (fieldName.equals(trTimeFieldName))
			{
				//System.out.println(assetCode + ":FieldID=" + fieldEntry.fieldId() + "FieldName=" + fieldEntry.name() + "," + "Value=" + fieldEntry.load());
				_quoteTime = fieldEntry.load().toString();
				SimpleDateFormat _sTime = new SimpleDateFormat("HH:mm:ss:SSS:SSS:SSS");
				SimpleDateFormat _sTime2 = new SimpleDateFormat("HH:mm:ss.SSSSSSSSS");
				try{
					Date _date = _sTime.parse(_quoteTime);
					_quoteTime = _sTime2.format(_date);
				} catch (ParseException e){
					System.out.println("trTimeFieldName: " + e.getMessage());
					break;
				}
				_currHour = _quoteTime.substring(0,2);
				_updateFlag++;
			}
			

		}
		if (_updateFlag == 2)
		{
			System.out.println(assetCode + ":" + thisActiveDate + "T" + _quoteTime + "," + _bidPrice + ",");
			// Whether or not data into BigQuery
			if (!_prevHour.equals(_currHour) && !_prevHour.equals("99"))
				{
					String __tmStamp = thisActiveDate + "T" + _quoteTime.substring(0,2) + ":00:00" + _quoteTime.substring(8,17) + "Z";
					// Time - 1 HOUR
					try{
						SimpleDateFormat __tmpTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'");
						Date __tmpDate = __tmpTime.parse(__tmStamp);
						Calendar __tmpDate2 = Calendar.getInstance();
						__tmpDate2.setTime(__tmpDate);
						Date __tmpDate3 = DateUtils.addHours(__tmpDate2.getTime(), -1);
						__tmStamp = __tmpTime.format(__tmpDate3);
					}
					catch(Exception e){
						
					}
					String __assetCode = assetCode.replace("/", "");
					String __priceRecord = _openPrice + "," + _highPrice + "," + _lowPrice + "," + _lastPrice;
					String __outputRecord = __tmStamp + "," + __assetCode + "," + __priceRecord;
					try
					{
						// Write Open/High/Low/Close into Symbol file
						File f = new File("./" + __assetCode + ".dat");
						BufferedWriter bw = new BufferedWriter(new FileWriter(f,true));
						// Header generation
						String __header = "windowTimestamp,assetCode,Open,High,Low,Last";
						bw.write(__header);
						bw.newLine();
						bw.write(__outputRecord);
						bw.newLine();
						bw.close();
						
						// Write EOJ file
						File ef = new File("./" + __assetCode + ".eoj");
						BufferedWriter ew = new BufferedWriter(new FileWriter(ef));
						ew.write("END");
						ew.close();
					} catch (IOException e)
					{
						System.out.println(e.getMessage());
					}
					
					System.out.println(__outputRecord);
				}
			// _midPrice = round((_bidPrice + _askPrice) / 2, 2);
			_midPrice = _bidPrice;
			if (!_prevHour.equals(_currHour))
			{
				_openPrice = _midPrice;
				_highPrice = -99999;
				_lowPrice = 99999;
			}
			if (_highPrice < _midPrice)
				_highPrice = _midPrice;
			if (_lowPrice > _midPrice)
				_lowPrice = _midPrice;
			_lastPrice = _midPrice;
			

			_prevHour = _currHour;
		}
			
	}

	
	private double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}

	
}
