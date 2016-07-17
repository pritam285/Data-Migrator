package com.yukthi.tools.db.template;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Properties;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;
import com.yukthi.ccg.xml.XMLBeanParser;
import com.yukthi.tools.db.connection.ConnectionUtils;
import com.yukthi.tools.db.data.migration.IDataStorage;
import com.yukthi.tools.db.exception.MigrationException;
import com.yukthi.tools.db.model.TableInfo;
import com.yukthi.tools.db.model.TableRow;

public class RdbmsDataStore implements IDataStorage
{
	/**
	 * The connection.
	 **/
	private Connection connection;

	/**
	 * The connection utils.
	 **/
	private ConnectionUtils connectionUtils;
	
	private RdbmsConfiguration rdbmsConfiguration; 
	
	public RdbmsDataStore(Properties properties)
	{
		this.connectionUtils = new ConnectionUtils();
		this.connection = connectionUtils.getConnection(properties);
		try
		{
			this.connection.setAutoCommit(false);
		} catch(SQLException e)
		{
			throw new MigrationException("Error occured while setting auto commit as false please check the settings", e);
		}
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
		
		Statement statement = null;
		
		try
		{
			statement = (Statement) connection.createStatement();
		} catch(SQLException ex)
		{
			throw new MigrationException("Error occured while creating statement from connection", ex);
		}
		
		for(TableInfo tableInfo : tableInfoList)
		{
			String createQuery = rdbmsConfiguration.buildQuery(RdbmsConfiguration.CREATE_QUERY, "tableInfo", tableInfo);
			
			//System.out.println(createQuery);
			
			/*try
			{
				statement.execute(createQuery);
			} catch(SQLException e)
			{
				try
				{
					// if any exception occurs while creating tables all the created
					// tables will be rolled back
					connection.rollback();
					
					throw new MigrationException("Error occured while creating table, " + tableInfo.getTableName(), e);
					
				} catch(SQLException ex)
				{
					throw new MigrationException("Error occured while rolling back the records", ex);
				}
			}*/
		}
	}

	@Override
	public void persist(TableInfo tableInfo, TableRow tableRow) 
	{
		String insertQuery = rdbmsConfiguration.buildQuery(RdbmsConfiguration.INSERT_QUERY, "tableRow", tableRow);
		
		System.out.println(insertQuery);
		
		PreparedStatement preparedStatement = null;
		
		int parameterIndex;
		
		Object[] values = tableRow.getRowValues();
		
		try
		{
			preparedStatement = (PreparedStatement) connection.prepareStatement(insertQuery);
			
			for(int i = 0; i < values.length; i++)
			{
				parameterIndex = i + 1;

				if(values[i] == null)
				{
					preparedStatement.setNull(parameterIndex, Types.NULL);
					continue;
				}
				else
				{
					preparedStatement.setObject(parameterIndex, values[i]);
				}
			}
			
			System.out.println(preparedStatement.toString());
			System.out.println(preparedStatement.execute());

		} catch(SQLException e)
		{
			try
			{
				// if any exception occurs while saving records
				// records will be rolled back
				connection.rollback();
				
				throw new MigrationException("Error occured while inserting records in table, " + tableRow.getTableName(), e);
				
			} catch(SQLException ex)
			{
				throw new MigrationException("Error occured while rolling back the records", ex);
			}
		}
	}

	@Override
	public void commit() 
	{
		
	}
}
