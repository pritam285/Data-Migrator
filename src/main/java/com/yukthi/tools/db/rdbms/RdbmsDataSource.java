package com.yukthi.tools.db.rdbms;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.DatabaseMetaData;
import com.mysql.jdbc.Statement;
import com.yukthi.tools.db.connection.ConnectionUtils;
import com.yukthi.tools.db.data.migration.IDataSource;
import com.yukthi.tools.db.data.migration.TableDetails;
import com.yukthi.tools.db.exception.MigrationException;
import com.yukthi.tools.db.model.Column;
import com.yukthi.tools.db.model.TableInfo;
import com.yukthi.tools.db.model.TableRow;

/**
 * The Class RdbmsDataSource. This class interacts with the data base for
 * reading the table information and its records.
 * 
 * @author Pritam
 */
public class RdbmsDataSource implements IDataSource
{
	/**
	 * The DbmsIterator inner class for iterating the records from the table.
	 */
	private class DbmsIterator implements Iterator<TableRow>
	{

		/**
		 * The table iterator.
		 **/
		private Iterator<TableInfo> tableIterator;

		/**
		 * The statement.
		 **/
		private Statement statement;

		/**
		 * The result set.
		 **/
		private ResultSet resultSet;

		/**
		 * The next row.
		 **/
		private TableRow nextRow = null;

		/**
		 * The table name.
		 **/
		private String tableName = null;

		/**
		 * Instantiates a new dbms iterator and gets the iterator from the table
		 * info list.
		 *
		 * @param tableInfoList
		 *            the table info list
		 */
		private DbmsIterator(List<TableInfo> tableInfoList)
		{
			this.tableIterator = tableInfoList.iterator();
		}

		/**
		 * Gets the result set from the statement object by using select query
		 * and table name.
		 *
		 * @param tableInfo
		 *            the table info
		 * @return the result set
		 */
		private ResultSet getResultSet(TableInfo tableInfo)
		{
			tableName = tableInfo.getTableName();

			try
			{
				statement = (Statement) connection.createStatement();
				resultSet = statement.executeQuery("SELECT * FROM " + tableName);

				return resultSet;
			} catch(SQLException e)
			{
				throw new NoSuchElementException("An error occurred while reading data from the table: " + tableName + e);
			}
		}

		/**
		 * Gets the next row, returns true if next value is present.
		 *
		 * @return the next row
		 */
		private boolean getNextRow()
		{
			// if row is already available (built by hasNext())
			if(nextRow != null)
			{
				return true;
			}

			if(resultSet != null)
			{
				try
				{
					if(resultSet.next())
					{
						getRowData();
						return true;
					}

					connectionUtils.closeResStat(resultSet, statement);
					resultSet = null;
				} catch(SQLException ex)
				{
					throw new NoSuchElementException("Error occurred while reading data from table" + ex);
				}
			}

			// loop through tables till table with records is found
			while(tableIterator.hasNext())
			{
				getResultSet(tableIterator.next());
				try
				{
					if(!resultSet.next())
					{
						connectionUtils.closeResStat(resultSet, statement);
						resultSet = null;
						continue;
					}
				} catch(SQLException ex)
				{
					throw new NoSuchElementException("Error occurred while reading data from table" + ex);
				}
				break;
			}

			if(resultSet == null)
			{
				return false;
			}

			getRowData();
			return true;
		}

		/**
		 * Gets the row data stores data in the table row.
		 *
		 * @return the row data
		 */
		private void getRowData()
		{
			nextRow = new TableRow();
			nextRow.setTableName(tableName);

			try
			{
				Object rowValues[] = new Object[resultSet.getMetaData().getColumnCount()];

				for(int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++)
				{
					rowValues[i - 1] = resultSet.getObject(i);
				}

				nextRow.setRowValues(rowValues);

			} catch(SQLException ex)
			{
				throw new NoSuchElementException("An error occurred while reading data from the table: " + tableName + ex);
			}
		}

		public boolean hasNext()
		{
			return getNextRow();
		}

		public TableRow next()
		{
			if(!getNextRow())
			{
				throw new NoSuchElementException();
			}

			TableRow res = nextRow;
			nextRow = null;

			return res;
		}
	}

	/**
	 * The table names.
	 **/
	private List<String> tableNames = new ArrayList<String>();;

	/**
	 * The tab nam tab detail map.
	 **/
	private Map<String, TableDetails> tabNamTabDetailMap = new HashMap<String, TableDetails>();

	/**
	 * The connection.
	 **/
	private Connection connection;

	/**
	 * The connection utils.
	 **/
	private ConnectionUtils connectionUtils;

	/**
	 * Instantiates a new rdbms data source.
	 *
	 * @param properties
	 *            the properties
	 */
	public RdbmsDataSource(Properties properties)
	{
		this.connectionUtils = new ConnectionUtils();
		this.connection = connectionUtils.getConnection(properties);
		this.getTables();
		this.getTableDetails();
	}

	/**
	 * Gets the tables.
	 *
	 * @return the tables
	 */
	private void getTables()
	{
		DatabaseMetaData databaseMetaData = null;
		ResultSet resultSet = null;

		try
		{
			databaseMetaData = (DatabaseMetaData) connection.getMetaData();

			resultSet = databaseMetaData.getTables(null, null, "%", null);

			while(resultSet.next())
			{
				tableNames.add(resultSet.getString("TABLE_NAME"));
			}
		} catch(SQLException e)
		{
			throw new MigrationException("Error occured while getting tables from the database", e);
		} finally
		{
			connectionUtils.closeResStat(resultSet, null);
		}
	}

	/**
	 * Gets the table details.
	 *
	 * @return the table details
	 */
	private void getTableDetails()
	{
		ResultSet resultSet = null;
		DatabaseMetaData databaseMetaData = null;

		List<String> parentTableName = null;
		List<String> primaryKeyList = null;
		Map<String, String[]> fkColNamPrntMap = null;
		String constraintNamParnt[] = null;
		Map<String, List<String>> constraintNamUKColNamMap = null;
		String constraintName = null;
		List<String> colNamsforUQList = null;
		List<Column> columnList = null;
		Column column = null;

		try
		{
			for(String tableName : tableNames)
			{
				parentTableName = new ArrayList<String>();
				primaryKeyList = new ArrayList<String>();
				fkColNamPrntMap = new HashMap<String, String[]>();
				constraintNamUKColNamMap = new HashMap<String, List<String>>();

				databaseMetaData = (DatabaseMetaData) connection.getMetaData();

				// Result set to get the primary key column name of the table
				resultSet = databaseMetaData.getPrimaryKeys(null, null, tableName);

				while(resultSet.next())
				{
					// getting the primary column name
					// assumed here that id is only the primary key, but for
					// composite primary key we need list.
					primaryKeyList.add(resultSet.getString("COLUMN_NAME"));
				}

				connectionUtils.closeResStat(resultSet, null);

				// Result set for getting the table details(foreign key, unique
				// key, parent table name).
				resultSet = databaseMetaData.getImportedKeys(connection.getCatalog(), null, tableName);

				while(resultSet.next())
				{
					// getting the parent table name
					parentTableName.add(resultSet.getString("PKTABLE_NAME"));

					// getting constraint name and parent table name
					constraintNamParnt = new String[] { resultSet.getString("FK_NAME"), resultSet.getString("PKTABLE_NAME") };

					// getting foreign key column name and constraint name
					fkColNamPrntMap.put(resultSet.getString("FKCOLUMN_NAME"), constraintNamParnt);
				}

				connectionUtils.closeResStat(resultSet, null);

				// Result set for getting the unique column details.
				resultSet = databaseMetaData.getIndexInfo(connection.getCatalog(), null, tableName, true, true);

				while(resultSet.next())
				{

					constraintName = resultSet.getString("INDEX_NAME");

					if(!(constraintName.equalsIgnoreCase("PRIMARY")))
					{
						if(constraintNamUKColNamMap.containsKey(constraintName))
						{
							colNamsforUQList = constraintNamUKColNamMap.get(constraintName);
							colNamsforUQList.add(resultSet.getString("COLUMN_NAME"));
						}
						else
						{
							colNamsforUQList = new ArrayList<String>();
							colNamsforUQList.add(resultSet.getString("COLUMN_NAME"));
						}
						constraintNamUKColNamMap.put(constraintName, colNamsforUQList);
					}
				}

				connectionUtils.closeResStat(resultSet, null);

				columnList = new ArrayList<Column>();

				// Result set for getting the column details.
				resultSet = databaseMetaData.getColumns(connection.getCatalog(), null, tableName, null);

				while(resultSet.next())
				{
					column = new Column();

					column.setLength(resultSet.getInt("COLUMN_SIZE"));
					column.setName(resultSet.getString("COLUMN_NAME"));
					column.setType(resultSet.getInt("DATA_TYPE"));
					column.setNullable(resultSet.getBoolean("IS_NULLABLE"));
					column.setAutoIncrement(resultSet.getBoolean("IS_AUTOINCREMENT"));
					column.setColumnTypeName(resultSet.getString("TYPE_NAME"));

					columnList.add(column);
				}

				tabNamTabDetailMap.put(tableName, new TableDetails(primaryKeyList, fkColNamPrntMap, parentTableName, constraintNamUKColNamMap, columnList));
			}
		} catch(SQLException ex)
		{
			throw new MigrationException("An error occured while getting the table details, ", ex);
		} finally
		{
			connectionUtils.closeResStat(resultSet, null);
		}
	}

	@Override
	public List<TableInfo> getTablesInOrder()
	{
		int indexChild, indexParent;

		List<String> parentTableNameList = null;

		String temp = null;

		String childName = null;

		int lastIndex = tableNames.size() - 1;

		for(int j = 0; j < tableNames.size(); j++)
		{
			for(int i = 0; i < tableNames.size(); i++)
			{
				childName = tableNames.get(i);

				parentTableNameList = tabNamTabDetailMap.get(childName).getParentTableNameList();

				if(parentTableNameList != null)
				{
					for(String parntName : parentTableNameList)
					{
						indexChild = i;

						indexParent = tableNames.indexOf(parntName);

						if(indexChild < indexParent)
						{
							temp = tableNames.get(indexChild);

							tableNames.set(indexChild, parntName);

							tableNames.set(indexParent, temp);

							break;
						}
					}
				}

				else
				{
					temp = tableNames.get(lastIndex);

					tableNames.set(lastIndex, tableNames.get(i));

					tableNames.set(i, temp);

					lastIndex--;
				}
			}

			lastIndex = tableNames.size() - 1;
		}

		return getTableInfoList();
	}

	/**
	 * Gets the table info list.
	 *
	 * @return the table info list
	 */
	private List<TableInfo> getTableInfoList()
	{
		List<TableInfo> tableInfoList = new ArrayList<TableInfo>();

		TableInfo tableInfo = null;
		TableDetails tableDetails = null;

		for(String tableName : tableNames)
		{
			tableInfo = new TableInfo();

			tableInfo.setTableName(tableName);

			tableDetails = tabNamTabDetailMap.get(tableName);

			tableInfo.setFkColNamPrntMap(tableDetails.getFkColNamPrntMap());
			tableInfo.setPkColumnNameList(tableDetails.getPkColumnNameList());
			tableInfo.setConstraintNamUKColNamMap(tableDetails.getConstraintNamUKColNamMap());
			tableInfo.setColumnList(tableDetails.getColumnList());

			tableInfoList.add(tableInfo);
		}

		return tableInfoList;
	}

	@Override
	public DbmsIterator getRecords(List<TableInfo> tableInfoList)
	{
		return new DbmsIterator(tableInfoList);
	}
	
	@Override
	public void closeResources()
	{
		connectionUtils.cleanUp();
	}
}
