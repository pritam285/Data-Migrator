package com.yukthi.tools.db.template;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.yukthi.ccg.util.CCGUtility;
import com.yukthi.tools.db.model.TableInfo;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class RdbmsConfiguration 
{
	public static String CREATE_QUERY = "createTableTemplate";

	public static String INSERT_QUERY = "insertRecordsTemplate";
	
	private Configuration configuration = new Configuration();
	
	private Map<String, String> queryTempMap = new HashMap<String, String>();
	
	private Map<String, Template> templateMap = new HashMap<>();
	
	private Map<String, Object> data;
	
	private boolean pagingSupported = true;

	public boolean isPagingSupported() 
	{
		return pagingSupported;
	}

	public void setPagingSupported(boolean pagingSupported) 
	{
		this.pagingSupported = pagingSupported;
	}
	
	public void addTemplate(String name, String template)
	{
		queryTempMap.put(name, template);
	} 
	
	public String buildQuery(String query, Object... objects)
	{
		return buildQuery(query, CCGUtility.buildMap(objects));
	}
	
	public String buildQuery(String query, Map<String, Object> data)
	{
		Template template = templateMap.get(query);
		
		if(template == null)
		{
			try 
			{
				template = new Template(query, queryTempMap.get(query), configuration);
				
				templateMap.put(query, template);
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
		
		StringWriter stringWriter = new StringWriter();
		
		try 
		{
			template.process(data, stringWriter);
			stringWriter.flush();
			stringWriter.close();
		} 
		catch (TemplateException | IOException e) 
		{
			e.printStackTrace();
		}
		
		return stringWriter.toString();
	}
}
