package com.dao.common;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Table;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.utils.ReadPropertiesFileUtil;

public abstract class CassandraDao<T> implements Connection, CommmonDAO<T> {

	private static final String DB_SCHEMA = "db.schema";
	private static final String DB_SERVER_IP = "db.server.ip";
	private Properties prop = ReadPropertiesFileUtil.readDbConfig();
	private Cluster cluster;
	private Session session;
	private Builder builder;

	private final Class<T> type;
	private HashMap<String, String> columns;

	enum JAVA_TYPE {
		_String(String.class), _long(long.class), _ByteBuffer(ByteBuffer.class), _boolean(
				boolean.class), _BigDecimal(BigDecimal.class), _double(
				double.class), _float(float.class), _InetAddress(
				InetAddress.class), _int(int.class), _List(List.class), _Map(
				Map.class), _Set(Set.class), _Date(Date.class), _BigInteger(
				BigInteger.class);

		Class<?> java_type;
		final static HashMap<Class<?>, JAVA_TYPE> values = new HashMap<Class<?>, CassandraDao.JAVA_TYPE>();

		static {
			for (JAVA_TYPE type : JAVA_TYPE.values()) {
				values.put(type.java_type, type);
			}
		}

		JAVA_TYPE(Class<?> type) {
			this.java_type = type;
		}

		static JAVA_TYPE getEnumType(Class<?> key) {
			if(values.containsKey(key)){
				return values.get(key);
			}else{
				return null;
			}
		}
	}

	public CassandraDao(Class<T> type) {
		this.type = type;
	}

	public Class<T> getType() {
		return this.type;
	}

	@Override
	public Connection connection() {
		return this;
	}

	@SuppressWarnings("unchecked")
	public CassandraDao() {
		this.type = (Class<T>) ((ParameterizedType) getClass()
				.getGenericSuperclass()).getActualTypeArguments()[0];
		builder = Cluster.builder();
		cluster = builder.addContactPoint(prop.getProperty(DB_SERVER_IP))
				.build();
		doConect(prop.getProperty(DB_SCHEMA));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dao.CommmonDAO#select(java.lang.String)
	 */
	@Override
	public List<T> select(String query) throws Exception {
		List<T> result = new ArrayList<T>();
		ResultSet resultSet = executeQuery(query);
		result = setDataToBean(resultSet);
		printData(resultSet.getColumnDefinitions(), result);
		return result;
	}

	@Override
	public void save(T bean) throws Exception {

		List<String> columnList = new ArrayList<String>();
		List<Object> paramList = new ArrayList<Object>();

		String tableName = getTableName(bean);
		getFieldAndParamFromBean(bean, columnList, paramList);

		Object param[] = new Object[paramList.size()];
		Arrays.fill(param, "?");

		StringBuffer preparedStatement = new StringBuffer();
		preparedStatement.append("INSERT INTO ");
		preparedStatement.append(tableName).append(" ( ")
				.append(StringUtils.join(columnList, ",")).append(" ) ");
		preparedStatement.append(" VALUES ");
		preparedStatement.append(" ( ")
				.append(StringUtils.join(paramList, ",")).append(" ) ");

		System.out.println("save preparedStatement:"
				+ preparedStatement.toString());
		System.out.println("values:" + paramList);
		ResultSet resultSet = executeQuery(preparedStatement.toString());
		System.out.println("result:" + resultSet);
	}

	@Override
	public void update(T bean) throws Exception {
		List<String> columnList = new ArrayList<String>();
		List<Object> paramList = new ArrayList<Object>();

		String tableName = getTableName(bean);
		getFieldAndParamFromBean(bean, columnList, paramList);

		StringBuffer preparedStatement = new StringBuffer();
		preparedStatement.append("UPDATE ");
		preparedStatement.append(tableName).append(" SET ");
		preparedStatement.append(generateSetUpdate(columnList, paramList));

		System.out.println("update preparedStatement:"
				+ preparedStatement.toString());
		System.out.println("values:" + paramList);
		ResultSet resultSet = executeQuery(preparedStatement.toString());
		System.out.println("result:" + resultSet);
	}

	@Override
	public void delete(T bean) throws Exception {
		List<String> columnList = new ArrayList<String>();
		List<Object> paramList = new ArrayList<Object>();

		// DELETE col1, col2, col3 FROM Planeteers WHERE userID = 'Captain';

		// DELETE FROM MastersOfTheUniverse WHERE mastersID IN ('Man-At-Arms',
		// 'Teela');

		String tableName = getTableName(bean);
		getFieldAndParamFromBean(bean, columnList, paramList);

		StringBuffer preparedStatement = new StringBuffer();
		preparedStatement.append("DELETE ");
		preparedStatement.append(tableName).append(" FROM ");
		preparedStatement.append(generateSetUpdate(columnList, paramList));

		System.out.println("delete preparedStatement:"
				+ preparedStatement.toString());
		System.out.println("values:" + paramList);
		ResultSet resultSet = executeQuery(preparedStatement.toString());
		System.out.println("result:" + resultSet);
	}

	private String generateSetUpdate(List<String> columnList,
			List<Object> paramList) {

		List<String> set = new ArrayList<String>();

		if (columnList == null) {
			return StringUtils.EMPTY;
		}

		int count = columnList.size();
		for (int index = 0; index < count; index++) {
			Object value = paramList.get(index);
			if (value == null) {
				continue;
			}
			StringBuffer result = new StringBuffer(StringUtils.EMPTY);
			result.append(columnList.get(index)).append("=").append(value);
			set.add(result.toString());
		}

		return StringUtils.join(set, ",");
	}

	// ================ public ==============

	public Session doConect() {
		session = cluster.connect();
		return session;
	}

	public Session doConect(String keyspaceName) {
		session = cluster.connect(keyspaceName);
		return session;
	}

	public ResultSet executeQuery(String query) {
		return session.execute(query);
	}

	// ================ private ==============

	/**
	 * get table name from bean
	 * 
	 * @param bean
	 * @return
	 */
	private String getTableName(T bean) {
		Table table = bean.getClass().getAnnotation(Table.class);
		String tableName = StringUtils.defaultString(table.name(), bean
				.getClass().getName());
		return tableName;
	}

	/**
	 * 
	 * 
	 * @param bean
	 * @param columnList
	 * @param paramList
	 */
	private void getFieldAndParamFromBean(T bean, List<String> columnList,
			List<Object> paramList) {
		Field[] fields = bean.getClass().getDeclaredFields();

		for (Field f : fields) {
			Column column = f.getAnnotation(Column.class);
			if (column == null) {
				System.out.println("ERROR " + f.getName()
						+ "does not have column anotation");
				continue;
			}
			String columnName = f.getName();

			Object value = getValueFromClass(bean, columnName);
			value = prepareValue(value);
			columnList.add(columnName);
			paramList.add(value);
		}
	}

	private Object prepareValue(Object value) {
		if (isString(value) && value != null) {
			value = "'" + value + "'";
		}
		return value;
	}

	private boolean isString(Object value) {
		boolean result = false;

		if (value instanceof Character) {
			result = true;
		} else if (value instanceof String) {
			result = true;
		}
		return result;
	}

	/**
	 * 
	 * set
	 * 
	 * @param resultSet
	 * @param clazz
	 * @return
	 * @throws Exception
	 */
	private List<T> setDataToBean(ResultSet resultSet) throws Exception {
		List<T> result = new ArrayList<T>();
		if (columns == null) {
			columns = getColumnMappingWithField();
		}
		for (Row row : resultSet) {
			T data = null;
			try {
				data = getType().newInstance();
				for (Definition column : resultSet.getColumnDefinitions()) {
					String columnName = column.getName();
					String fieldName = columns.containsKey(columnName) ? columns
							.get(columnName) : null;

					setDataToAttribute(data, fieldName,
							row.getObject(columnName));
				}
				result.add(data);
			} catch (Exception e) {
				throw e;
			}
		}

		return result;
	}

	/**
	 * 
	 * Mapping field between data base and bean.
	 * 
	 * Key = column name <br/>
	 * Value = field name
	 * 
	 * @param clazz
	 * @return
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws SecurityException
	 */
	private HashMap<String, String> getColumnMappingWithField()
			throws SecurityException, InstantiationException,
			IllegalAccessException {
		HashMap<String, String> result = new HashMap<String, String>();
		Field[] fields = getType().newInstance().getClass().getDeclaredFields();

		for (Field f : fields) {
			Column column = f.getAnnotation(Column.class);
			String key = "";
			String value = f.getName();
			if (column == null) {
				key = f.getName();
			} else {
				key = column.name();
			}
			result.put(key, value);
		}

		return result;
	}

	/**
	 * set value to field
	 * 
	 * @param clazz
	 * @param fieldName
	 * @param value
	 * @return
	 */
	private T setDataToAttribute(T clazz, String fieldName, Object value) {

		Field f;
		try {
			f = clazz.getClass().getDeclaredField(fieldName);
			f.setAccessible(true);// Very important, this allows the setting to
			// work.
			f.set(clazz, value);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return clazz;
	}

	private void printData(ColumnDefinitions columnDefinitions, List<T> listData)
			throws SecurityException, InstantiationException,
			IllegalAccessException {

		for (Definition column : columnDefinitions) {
			System.out.print(column.getName() + "|");
		}
		System.out.println("");
		if (columns == null) {
			columns = getColumnMappingWithField();
		}
		for (T data : listData) {
			for (Definition column : columnDefinitions) {
				System.out.print(getValueFromClass(data,
						columns.get(column.getName()))
						+ "|");
			}
			System.out.println("");
		}
	}

	private Object getValueFromClass(T clazz, String columnName) {
		Object result = null;
		Field f;
		try {
			f = clazz.getClass().getDeclaredField(columnName);
			f.setAccessible(true);// Very important, this allows the setting to
			// work.
			result = f.get(clazz);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return result;
	}

}
