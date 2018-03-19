package com.thomsonreuters.gcp.feedPrc4Trmi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;

import com.thomsonreuters.ema.access.AckMsg;
import com.thomsonreuters.ema.access.FieldEntry;
import com.thomsonreuters.ema.access.FieldList;
import com.thomsonreuters.ema.access.GenericMsg;
import com.thomsonreuters.ema.access.Msg;
import com.thomsonreuters.ema.access.OmmConsumerClient;
import com.thomsonreuters.ema.access.OmmConsumerEvent;
import com.thomsonreuters.ema.access.RefreshMsg;
import com.thomsonreuters.ema.access.StatusMsg;
import com.thomsonreuters.ema.access.UpdateMsg;

public class emaAppClient2 implements OmmConsumerClient {
	double _bidPrice;
	double _askPrice;
	double _midPrice;
	
	double _openPrice;
	double _highPrice;
	double _lowPrice;
	double _lastPrice;
	
	String _prevHour;

	@Override
	public void onAckMsg(AckMsg arg0, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onAllMsg(Msg arg0, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onGenericMsg(GenericMsg arg0, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onRefreshMsg(RefreshMsg arg0, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onStatusMsg(StatusMsg arg0, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onUpdateMsg(UpdateMsg arg0, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub

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
			//System.out.println("FieldID=" + fieldEntry.fieldId() + "FieldName=" + fieldEntry.name() + "," + "Value=" + fieldEntry.load());
			if (fieldName.equals("BID") || fieldName.equals("ASK"))
			{
				if (fieldName.equals("BID"))
				{
					_bidPrice = Double.parseDouble(fieldEntry.load().toString());
					_updateFlag++;
				}
				else
				{
					_askPrice = Double.parseDouble(fieldEntry.load().toString());
					_updateFlag++;
				}
			}
			
			if (fieldName.equals("ACTIV_DATE"))
			{
				_activeDate = fieldEntry.load().toString();
				SimpleDateFormat _sDate = new SimpleDateFormat("dd MMM yyyy", Locale.ENGLISH);
				SimpleDateFormat _sDate2 = new SimpleDateFormat("yyyy-MM-dd");
				try {
					Date _date = _sDate.parse(_activeDate);
					_activeDate = _sDate2.format(_date);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				_updateFlag++;
			}
			if (fieldName.equals("QUOTIM"))
			{
				_quoteTime = fieldEntry.load().toString();
				SimpleDateFormat _sTime = new SimpleDateFormat("HH:mm:ss:SSS:SSS:SSS");
				SimpleDateFormat _sTime2 = new SimpleDateFormat("HH:mm:ss.SSSSSSSSS");
				try{
					Date _date = _sTime.parse(_quoteTime);
					_quoteTime = _sTime2.format(_date);
				} catch (ParseException e){
					e.printStackTrace();
				}
				_currHour = _quoteTime.substring(0,2);
				_updateFlag++;
			}
			

		}
		if (_updateFlag == 4)
		{
			// Whether or not data into BigQuery
			if (!_prevHour.equals(_currHour) && !_prevHour.equals("99"))
				{
					String __tmStamp = _activeDate + "T" + _quoteTime.substring(0,2) + ":00:00" + _quoteTime.substring(8,17) + "Z";
					String __assetCode = assetCode.replace("/", "");
					String __priceRecord = _openPrice + "," + _highPrice + "," + _lowPrice + "," + _lastPrice;
					String __outputRecord = __tmStamp + "," + __assetCode + "," + __priceRecord;
					try
					{
						// Write Open/High/Low/Close into Symbol file
						File f = new File("./" + __assetCode + ".dat");
						BufferedWriter bw = new BufferedWriter(new FileWriter(f));
						// Header generation
						String __header = "windowTimestamp,assetCode,openPrice,highPrice,lowPrice,lastPrice";
						bw.write(__header);
						bw.newLine();
						bw.write(__outputRecord);
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
