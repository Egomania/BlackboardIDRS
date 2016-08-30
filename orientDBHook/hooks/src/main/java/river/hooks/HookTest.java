package river.hooks;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Set;

import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class HookTest extends ODocumentHookAbstract implements ORecordHook {
  public HookTest() {
    setExcludeClasses("Log"); //if comment out this one line or leave off the constructor entirely then OrientDB fails on every command
  }

	private String serverURL;
	private List<String> tables = new ArrayList<String>();
	private String channel;
	
	public void config(OServerParameterConfiguration[] iParams) 
	{
		for (OServerParameterConfiguration param : iParams) 
		{
	    	if (param.name.equalsIgnoreCase("serverURL")) 
			{
	      			serverURL = param.value;
	    	}
	    	if (param.name.equalsIgnoreCase("channel")) 
			{
	      			channel = param.value;
	    	}
			if (param.name.equalsIgnoreCase("tables")) 
			{
				String[] parts = param.value.split(":");
				for (String part: parts) 
				{
					String partLower = part.toLowerCase();
    				tables.add(partLower);
				}
			}
  		}
	}

	  @Override
	  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
		return DISTRIBUTED_EXECUTION_MODE.BOTH;
	  }

	private Boolean isIn(ODocument list, String elem){
		for( String field : list.getDirtyFields() ) {
			if (field.equals(elem))
				return true;
		}
		return false;
	}

	private void send(String content, ODocument document) throws Exception{
		
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpPost httpPost = new HttpPost(serverURL);

		List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();
		nameValuePairs.add(new BasicNameValuePair("operation", content));

		String table = document.getClassName();
		nameValuePairs.add(new BasicNameValuePair("table", table));
		nameValuePairs.add(new BasicNameValuePair("rid", document.getIdentity().toString()));
		for( String field : document.fieldNames() ) {

			Object value = document.field(field); 
			Object originalValue = null;
			if (isIn(document,field))
				{originalValue = document.getOriginalValue(field);}
			else
				{originalValue = value;}
			
			String ov, v;
			if (originalValue != null)
				ov = originalValue.toString();
			else
				ov = "None";
			if (value != null)
				v = value.toString();
			else
				v = "None";

			nameValuePairs.add(new BasicNameValuePair(field, v));
			nameValuePairs.add(new BasicNameValuePair(field+"_original", ov));
		}

		httpPost.setEntity(new UrlEncodedFormEntity(nameValuePairs));
		CloseableHttpResponse response = httpclient.execute(httpPost);
		
		try {
			//System.out.println(response.getStatusLine());
			HttpEntity entity2 = response.getEntity();
			EntityUtils.consume(entity2);
		} finally {
			response.close();
		}

	}

  public void onRecordAfterCreate( ODocument iDocument ) {
	ODocument document = (ODocument) iDocument;
	//System.out.println("Ran create hook " + document.getClassName());
	String search = document.getClassName().toLowerCase();
	if (tables.contains(search))
	{
		try { send("insert", document); }
		catch (Exception e) {System.err.println("Caught Exception: " + e.getMessage());}
		/*if (channel.equals("RestAPI"))
		{
			try { send("insert", document); }
			catch (Exception e) {System.err.println("Caught Exception: " + e.getMessage());}
		}
		else if (channel.equals("SOAP"))
		{
			throw new IllegalArgumentException("Invalid channel definition: " + channel);
		}
		else
		{
			throw new IllegalArgumentException("Invalid channel definition: " + channel);
		}*/
	}
  }

  public void onRecordAfterUpdate( ODocument iDocument ) {
	ODocument document = (ODocument) iDocument;
	//System.out.println("Ran update hook " + document.getClassName());
	String search = document.getClassName().toLowerCase();
	if (tables.contains(search))
	{
		try { send("update", document); }
		catch (Exception e) {System.err.println("Caught Exception: " + e.getMessage());}
		/*if (channel.equals("RestAPI"))
		{
			try { send("update", document); }
			catch (Exception e) {System.err.println("Caught Exception: " + e.getMessage());}
		}
		else if (channel.equals("SOAP"))
		{
			throw new IllegalArgumentException("Invalid channel definition: " + channel);
		}
		else
		{
			throw new IllegalArgumentException("Invalid channel definition: " + channel);
		}*/
	}
  }

  public void onRecordAfterDelete( ODocument iDocument ) {
	ODocument document = (ODocument) iDocument;
	//System.out.println("Ran delete hook " + document.getClassName());
	String search = document.getClassName().toLowerCase();
	if (tables.contains(search))
	{
		try { send("delete", document); }
		catch (Exception e) {System.err.println("Caught Exception: " + e.getMessage());}
		/*if (channel.equals("RestAPI"))
		{
			try { send("delete", document); }
			catch (Exception e) {System.err.println("Caught Exception: " + e.getMessage());}
		}
		else if (channel.equals("SOAP"))
		{
			throw new IllegalArgumentException("Invalid channel definition: " + channel);
		}
		else
		{
			throw new IllegalArgumentException("Invalid channel definition: " + channel);
		}*/
	}
  }

}
