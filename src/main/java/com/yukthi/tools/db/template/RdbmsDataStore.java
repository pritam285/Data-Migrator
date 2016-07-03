package com.yukthi.tools.db.template;

import java.util.List;
import java.util.Properties;

import com.yukthi.ccg.xml.XMLBeanParser;
import com.yukthi.tools.db.data.migration.IDataStorage;
import com.yukthi.tools.db.model.TableInfo;
import com.yukthi.tools.db.model.TableRow;

public class RdbmsDataStore implements IDataStorage
{
	private RdbmsConfiguration rdbmsConfiguration; 
	
	public RdbmsDataStore(Properties properties)
	{
		
	}
	
	private void loadXml(String templateName)
	{
		rdbmsConfiguration = new RdbmsConfiguration();
		
		XMLBeanParser.parse(RdbmsDataStore.class.getResourceAsStream("/" + templateName + ".xml"), rdbmsConfiguration);
	}

	@Override
	public void createTables(List<TableInfo> tableInfoList) 
	{
		loadXml("com/yukthi/tools/db/template/testmysql");
		
		for(TableInfo tableInfo : tableInfoList)
		{
			String str = rdbmsConfiguration.buildQuery(RdbmsConfiguration.CREATE_QUERY, "tableInfo", tableInfo);
			
			System.out.println(str);
		}
	}

	@Override
	public void persist(TableInfo tableInfo, TableRow tableRow) 
	{
		
	}

	@Override
	public void commit() 
	{
		
	}
}
