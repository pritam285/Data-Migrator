package com.yukthi.tools.db.rdbms;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;
import com.yukthi.tools.db.connection.ConnectionUtils;
import com.yukthi.tools.db.data.migration.IDataStorage;
import com.yukthi.tools.db.exception.MigrationException;
import com.yukthi.tools.db.model.Column;
import com.yukthi.tools.db.model.TableInfo;
import com.yukthi.tools.db.model.TableRow;

/**
 * The Class RdbmsDataStorage receives data from the data migration manager and
 * save it into the database
 * 
 * @author Pritam.
 */
public class RdbmsDataStorage implements IDataStorage
{
	/**
	 * The connection.
	 **/
	private Connection connection;

	/**
	 * The connection utils.
	 **/
	private ConnectionUtils connectionUtils;

	/**
	 * Instantiates a new rdbms data storage gets the connection object and set
	 * it auto commit false.
	 *
	 * @param properties
	 *            the properties
	 */
	public RdbmsDataStorage(Properties properties)
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

	/**
	 * Creates all the tables present in the list.
	 */
	@Override
	public void createTables(List<TableInfo> tableInfoList)
	{
		Statement statement = null;
		StringBuilder createQuery = null;
		String tableName = null;
		List<Column> columnList = null;
		Column column = null;
		Map<String, List<String>> constraintNamUKColNamMap = null;
		Set<String> set = null;
		Iterator<String> iterator = null;
		String constraintName = null;
		List<String> colNamsforUQList = null;
		Map<String, String[]> fkColNamPrntMap = null;
		String fkColName = null;
		String constraintNamParnt[] = null;

		try
		{
			statement = (Statement) connection.createStatement();
		} catch(SQLException ex)
		{
			throw new MigrationException("Error occured while creating statement from connection", ex);
		}
		
		for(TableInfo tableInfo : tableInfoList)
		{
			createQuery = new StringBuilder("CREATE TABLE ");

			tableName = tableInfo.getTableName();

			createQuery.append(tableName.toUpperCase() + "(");

			columnList = tableInfo.getColumnList();

			for(int i = 0; i < columnList.size(); i++)
			{
				column = columnList.get(i);

				// comma for next values
				if(i != 0)
				{
					createQuery.append(",");
				}

				createQuery.append(column.getName() + " " + column.getColumnTypeName());

				// checking for varchar for length
				if(column.getColumnTypeName().equalsIgnoreCase("varchar"))
				{
					createQuery.append("(" + column.getLength() + ")");
				}

				// if column is primary key
				if(tableInfo.getPkColumnNameList().contains(column.getName()))
				{
					createQuery.append(" PRIMARY KEY");
				}

				// if column is not null
				if(column.getNullable() == false)
				{
					createQuery.append(" NOT NULL");
				}

				// if column is auto increment
				if(column.isAutoIncrement())
				{
					createQuery.append(" AUTO_INCREMENT");
				}
			}

			constraintNamUKColNamMap = tableInfo.getConstraintNamUKColNamMap();

			// checks for unique key
			if(constraintNamUKColNamMap != null)
			{
				set = constraintNamUKColNamMap.keySet();
				iterator = set.iterator();

				while(iterator.hasNext())
				{
					constraintName = iterator.next();

					createQuery.append(",CONSTRAINT " + constraintName + " UNIQUE (");

					colNamsforUQList = constraintNamUKColNamMap.get(constraintName);

					for(int i = 0; i < colNamsforUQList.size(); i++)
					{
						if(i > 0)
						{
							createQuery.append(",");
						}

						createQuery.append(colNamsforUQList.get(i));
					}
					createQuery.append(")");
				}
			}

			fkColNamPrntMap = tableInfo.getFkColNamPrntMap();

			// checks for foreign key
			if(fkColNamPrntMap != null)
			{
				set = fkColNamPrntMap.keySet();
				iterator = set.iterator();

				while(iterator.hasNext())
				{
					fkColName = iterator.next();

					constraintNamParnt = fkColNamPrntMap.get(fkColName);

					createQuery.append(",CONSTRAINT " + constraintNamParnt[0] + " FOREIGN KEY (" + fkColName + ") REFERENCES  " + constraintNamParnt[1] + "(ID)");
				}
			}

			createQuery.append(") CHARACTER SET = 'LATIN1'");
			
			try
			{
				statement.execute(createQuery.toString());
			} catch(SQLException e)
			{
				try
				{
					// if any exception occurs while creating tables all the created
					// tables will be rolled back
					connection.rollback();
					
					throw new MigrationException("Error occured while creating tabale, " + tableName, e);
					
				} catch(SQLException ex)
				{
					throw new MigrationException("Error occured while rolling back the records", ex);
				}
			}
		}
		connectionUtils.closeResStat(null, statement);
	}

	/**
	 * Receives table row which has records and insert it to the respective
	 * table.
	 */
	@Override
	public void persist(TableInfo tableInfo, TableRow tableRow)
	{
		int parameterIndex = 0;
		PreparedStatement preparedStatement = null;

		StringBuilder insertQuery = new StringBuilder("INSERT INTO ");

		insertQuery.append(tableRow.getTableName() + " VALUES (");

		Object values[] = tableRow.getRowValues();

		// loop for ? prepared statement
		for(int i = 0; i < values.length; i++)
		{
			if(i > 0)
			{
				insertQuery.append(",");
			}

			insertQuery.append("?");
		}

		insertQuery.append(")");

		try
		{
			preparedStatement = (PreparedStatement) connection.prepareStatement(insertQuery.toString());

			// loop for values
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
			preparedStatement.execute();

		} catch(SQLException e)
		{
			try
			{
				connection.rollback();
			} catch(SQLException ex)
			{
				throw new MigrationException("Error occured while rolling back the records", ex);
			}
		}
		connectionUtils.closeResStat(null, preparedStatement);
	}

	@Override
	public void commit()
	{
		try
		{
			connection.commit();
		} catch(SQLException e)
		{
			throw new MigrationException("Error occured while committing the records", e);
		}
		connectionUtils.cleanUp();
	}
}
