package com.yukthi.tools.db.model;

import java.io.Serializable;

/**
 * The serializable class Column to store column details into the file. 
 * 
 * @author Pritam
 */
public class Column implements Serializable
{
	
	/** 
	 * The Constant serialVersionUID. 
	 **/
	private static final long serialVersionUID = 8029064582081018527L;

	/** 
	 * The auto increment. 
	 **/
	private Boolean autoIncrement;
	
	/** 
	 * The name. 
	 **/
	private String name;
	
	/** 
	 * The type. 
	 **/
	private Integer type;
	
	/** 
	 * The length. 
	 **/
	private Integer length;
	
	/** 
	 * The precision. 
	 **/
	private Integer precision;
	
	/** 
	 * The column type name. 
	 **/
	private String columnTypeName;
	
	/** 
	 * The nullable. 
	 **/
	private Boolean nullable ;

	/**
	 * Gets the name.
	 *
	 * @return the name
	 */
	public String getName()
	{
		return name;
	}

	/**
	 * Sets the name.
	 *
	 * @param name the new name
	 */
	public void setName(String name)
	{
		this.name = name;
	}

	/**
	 * Gets the type.
	 *
	 * @return the type
	 */
	public Integer getType()
	{
		return type;
	}

	/**
	 * Sets the type.
	 *
	 * @param type the new type
	 */
	public void setType(Integer type)
	{
		this.type = type;
	}

	/**
	 * Gets the length.
	 *
	 * @return the length
	 */
	public Integer getLength()
	{
		return length;
	}

	/**
	 * Sets the length.
	 *
	 * @param length the new length
	 */
	public void setLength(Integer length)
	{
		this.length = length;
	}

	/**
	 * Gets the precision.
	 *
	 * @return the precision
	 */
	public Integer getPrecision()
	{
		return precision;
	}

	/**
	 * Sets the precision.
	 *
	 * @param precision the new precision
	 */
	public void setPrecision(Integer precision)
	{
		this.precision = precision;
	}

	/**
	 * Gets the column type name.
	 *
	 * @return the column type name
	 */
	public String getColumnTypeName()
	{
		return columnTypeName;
	}

	/**
	 * Sets the column type name.
	 *
	 * @param columnTypeName the new column type name
	 */
	public void setColumnTypeName(String columnTypeName)
	{
		this.columnTypeName = columnTypeName;
	}

	/**
	 * Gets the nullable.
	 *
	 * @return the nullable
	 */
	public Boolean getNullable()
	{
		return nullable;
	}

	/**
	 * Sets the nullable.
	 *
	 * @param nullable the new nullable
	 */
	public void setNullable(Boolean nullable)
	{
		this.nullable = nullable;
	}

	/**
	 * Sets the auto increment.
	 *
	 * @param autoIncrement the new auto increment
	 */
	public void setAutoIncrement(Boolean autoIncrement)
	{
		this.autoIncrement = autoIncrement;
	}
	
	/**
	 * Checks if is auto increment.
	 *
	 * @return the boolean
	 */
	public Boolean isAutoIncrement()
	{
		return autoIncrement;
	}

	public Boolean getAutoIncrement() 
	{
		return autoIncrement;
	}
}
