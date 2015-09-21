package com.test.cassandra.dao.common;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.utils.ReadPropertiesFileUtil;

public abstract class CassandraDao<T> implements Connection, CommmonDAO<T> {

	private static final String ALLOW_FILTERING = " ALLOW FILTERING ";
	private static final String STAR_SYMBOL = "*";
	private static final String SELECT = "SELECT ";
	private static final String INSERT_INTO = "INSERT INTO ";
	private static final String VALUES = " VALUES ";
	private static final String DELETE = "DELETE ";
	private static final String UPDATE = "UPDATE ";
	private static final String SET = " SET ";
	private static final String WHERE = " WHERE ";
	private static final String FROM = " FROM ";
	private static final String AND = " AND ";
	private static final String COMMA_SYMBOL = ",";
	private static final String EQUAL_SYMBOL = "=";
	private static final String DB_SCHEMA = "db.schema";
	private static final String DB_SERVER_IP = "db.server.ip";
	private Properties prop = ReadPropertiesFileUtil.readDbConfig();
	private Cluster cluster;
	private Session session;
	private Builder builder;

	private Set<String> primaryColumn = new HashSet<String>();

	private final Class<T> type;
	private HashMap<String, String> columns;

	enum METHOD_TYPE {
		_SAVE, _UPDATE, _SELECT, _DELETE, _CREATE;
	}

	enum JAVA_TYPE {
		_String(String.class), _long(long.class), _ByteBuffer(ByteBuffer.class), _boolean(
				boolean.class), _BigDecimal(BigDecimal.class), _double(
				double.class), _float(float.class), _InetAddress(
				InetAddress.class), _int(int.class), _List(List.class), _Map(
				Map.class), _Set(Set.class), _Date(Date.class), _BigInteger(
				BigInteger.class), _UUIDs(UUIDs.class), _blob(Blob.class);

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
			if (values.containsKey(key)) {
				return values.get(key);
			} else {
				return null;
			}
		}
	}

	/**
	 * 
	 * 
	 * <b>Type Constants supported Description</b> <br>
	 * ascii strings ASCII character string <br>
	 * bigint integers 64-bit signed long<br>
	 * blob blobs Arbitrary bytes (no validation)<br>
	 * boolean booleans true or false<br>
	 * counter integers Counter column (64-bit signed value). See Counters for
	 * details<br>
	 * decimal integers, floats Variable-precision decimal<br>
	 * double integers 64-bit IEEE-754 floating point<br>
	 * float integers, floats 32-bit IEEE-754 floating point<br>
	 * inet strings An IP address. It can be either 4 bytes long (IPv4) or 16
	 * bytes long (IPv6). There is no inet constant, IP address should be
	 * inputed as strings<br>
	 * int integers 32-bit signed int<br>
	 * text strings UTF8 encoded string<br>
	 * timestamp integers, strings A timestamp. Strings constant are allow to
	 * input timestamps as dates, see Working with dates below for more
	 * information.<br>
	 * timeuuid uuids Type 1 UUID. This is generally used as a “conflict-free”
	 * timestamp. Also see the functions on Timeuuid<br>
	 * uuid uuids Type 1 or type 4 UUID<br>
	 * varchar strings UTF8 encoded string<br>
	 * varint integers Arbitrary-precision integer<br>
	 * 
	 * @author Alien
	 *
	 */
	enum CQL_TYPE {
		_uuid("uuid"), _blob("blob"), _boolean("boolean"), _decimal("decimal"), _text(
				"text"), _varchar("varchar"), _int("int"), _varint("varint");

		String cql_type;
		final static HashMap<String, CQL_TYPE> values = new HashMap<String, CassandraDao.CQL_TYPE>();

		static {
			for (CQL_TYPE type : CQL_TYPE.values()) {
				values.put(type.cql_type, type);
			}
		}

		CQL_TYPE(String type) {
			this.cql_type = type;
		}

		static CQL_TYPE getEnumType(String key) {
			if (values.containsKey(key)) {
				return values.get(key);
			} else {
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
	public CassandraDao() throws SecurityException, InstantiationException,
			IllegalAccessException {
		this.type = (Class<T>) ((ParameterizedType) getClass()
				.getGenericSuperclass()).getActualTypeArguments()[0];
		getColumnMappingWithField();

		builder = Cluster.builder();
		cluster = builder.addContactPoint(prop.getProperty(DB_SERVER_IP))
				.build();
		doConect(prop.getProperty(DB_SCHEMA));
	}

	public void createTable(T bean) {
		//
		// List<String> columnList = new ArrayList<String>();
		//
		// String tableName = getTableName(bean);
		// getFieldCreate(bean, columnList);
		//
		// StringBuffer preparedStatement = new StringBuffer();
		// preparedStatement.append("CREATE TABLE ");
		// preparedStatement.append(tableName).append(" ( ")
		// .append(StringUtils.join(columnList, ",")).append(" ) ");
		// preparedStatement.append(" VALUES ");
		// preparedStatement.append(" ( ")
		// .append(StringUtils.join(paramList, ",")).append(" ) ");
		//
		// System.out.println("save preparedStatement:"
		// + preparedStatement.toString());
		// System.out.println("values:" + paramList);
		// ResultSet resultSet = executeQuery(preparedStatement.toString());
		// System.out.println("result:" + resultSet);

		/*
		 * 
		 * 
		 * CREATE TABLE test ( pk int, t int, v text, s text static, PRIMARY KEY
		 * (pk, t) );
		 * 
		 * 
		 * CREATE TABLE company.emp ( emp_id timeuuid PRIMARY KEY, emp_city
		 * text, emp_email text, emp_last_name text, emp_name text, emp_phone
		 * text, emp_salary varint )
		 * 
		 * 
		 * WITH bloom_filter_fp_chance = 0.01 AND caching = '{"keys":"ALL",
		 * "rows_per_partition":"NONE"}' AND comment = '' AND compaction =
		 * {'class':
		 * 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
		 * AND compression = {'sstable_compression':
		 * 'org.apache.cassandra.io.compress.LZ4Compressor'} AND
		 * dclocal_read_repair_chance = 0.1 AND default_time_to_live = 0 AND
		 * gc_grace_seconds = 864000 AND max_index_interval = 2048 AND
		 * memtable_flush_period_in_ms = 0 AND min_index_interval = 128 AND
		 * read_repair_chance = 0.0 AND speculative_retry = '99.0PERCENTILE';
		 * 
		 * CREATE INDEX name ON company.emp (emp_name);
		 */
	}

	@Override
	public List<T> select(T bean) throws Exception {
		List<T> result = new ArrayList<T>();
		List<String> columnList = new ArrayList<String>();
		List<Object> paramList = new ArrayList<Object>();

		String tableName = getTableName(bean);
		HashMap<Field, Object> fieldsMap = getFieldAndParamFromBean(
				METHOD_TYPE._SELECT, bean, columnList, paramList);

		Object param[] = new Object[paramList.size()];
		Arrays.fill(param, "?");

		StringBuffer preparedStatement = new StringBuffer();
		preparedStatement.append(SELECT).append(STAR_SYMBOL).append(FROM);
		preparedStatement.append(tableName).append(" ");

		if (hashId(fieldsMap)) {
			preparedStatement.append(generateWhereCauseById(fieldsMap));
		} else {
			preparedStatement.append(generateWhereCause(columnList, paramList));
		}

		result = select(preparedStatement.toString());
		return result;
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
		getFieldAndParamFromBean(METHOD_TYPE._SAVE, bean, columnList, paramList);

		Object param[] = new Object[paramList.size()];
		Arrays.fill(param, "?");

		StringBuffer preparedStatement = new StringBuffer();
		preparedStatement.append(INSERT_INTO);
		preparedStatement.append(tableName).append(" ( ")
				.append(StringUtils.join(columnList, COMMA_SYMBOL))
				.append(" ) ");
		preparedStatement.append(VALUES);
		preparedStatement.append(" ( ")
				.append(StringUtils.join(paramList, COMMA_SYMBOL))
				.append(" ) ");
		save(preparedStatement.toString());
	}

	@Override
	public void save(String query) throws Exception {
		System.out.println("save query:" + query);
		ResultSet resultSet = executeQuery(query);
		System.out.println("result:" + resultSet);
	}

	@Override
	public void update(T bean) throws Exception {
		List<String> columnList = new ArrayList<String>();
		List<Object> paramList = new ArrayList<Object>();

		String tableName = getTableName(bean);
		HashMap<Field, Object> fieldsMap = getFieldAndParamFromBean(
				METHOD_TYPE._UPDATE, bean, columnList, paramList);

		StringBuffer preparedStatement = new StringBuffer();
		preparedStatement.append(UPDATE);
		preparedStatement.append(tableName).append(SET);
		preparedStatement.append(generateSetUpdate(columnList, paramList));
		preparedStatement.append(tableName);
		if (hashId(fieldsMap)) {
			preparedStatement.append(generateWhereCauseById(fieldsMap));
		} else {
			preparedStatement.append(generateWhereCause(columnList, paramList));
		}

		update(preparedStatement.toString());
	}

	@Override
	public void update(String query) throws Exception {
		System.out.println("update query:" + query);
		ResultSet resultSet = executeQuery(query);
		System.out.println("result:" + resultSet);
	}

	@Override
	public void delete(T bean) throws Exception {
		delete(null, bean);
	}

	public void delete(List<String> column, T bean) throws Exception {
		List<String> columnList = new ArrayList<String>();
		List<Object> paramList = new ArrayList<Object>();

		String deleteColumn = StringUtils.EMPTY;
		if (!Objects.isNull(column)) {
			deleteColumn = StringUtils.defaultString(
					StringUtils.join(column, COMMA_SYMBOL), StringUtils.EMPTY);
		}

		String tableName = getTableName(bean);
		HashMap<Field, Object> fieldsMap = getFieldAndParamFromBean(
				METHOD_TYPE._DELETE, bean, columnList, paramList);

		StringBuffer preparedStatement = new StringBuffer();
		preparedStatement.append(DELETE);
		preparedStatement.append(deleteColumn);
		preparedStatement.append(FROM).append(tableName);
		if (hashId(fieldsMap)) {
			preparedStatement.append(generateWhereCauseById(fieldsMap));
		} else {
			preparedStatement.append(generateWhereCause(columnList, paramList));
		}

		delete(preparedStatement.toString());
	}

	@Override
	public void delete(String query) throws Exception {
		System.out.println("delete query:" + query);
		ResultSet resultSet = executeQuery(query);
		System.out.println("result:" + resultSet);
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

	// ================ private ==============

	private ResultSet executeQuery(String query) {
		System.out.println("Fninal query:" + query);
		return session.execute(query);
	}

	private boolean hashId(HashMap<Field, Object> fields) {
		boolean result = false;
		for (Entry<Field, Object> set : fields.entrySet()) {
			Field field = set.getKey();
			Id id = field.getAnnotation(Id.class);

			if (Objects.isNull(id)) {
				continue;
			}
			Object value = set.getValue();
			if (!Objects.isNull(value)) {
				return true;
			}
		}
		return result;
	}

	/**
	 * Generate where cause by value not null.
	 * 
	 * @param columnList
	 * @param paramList
	 * @return
	 */
	private String generateWhereCause(List<String> columnList,
			List<Object> paramList) {
		StringBuilder result;
		List<String> whereCause = new ArrayList<String>();

		for (int index = 0; index < columnList.size(); index++) {
			String colunmName = columnList.get(index);
			Object value = paramList.get(index);

			if (Objects.isNull(value)) {
				continue;
			}
			StringBuffer condition = new StringBuffer();
			condition.append(colunmName).append(EQUAL_SYMBOL).append(value);
			whereCause.add(condition.toString());
		}

		result = new StringBuilder(StringUtils.join(whereCause, AND));
		if (StringUtils.isNotEmpty(result.toString())) {
			result.insert(0, WHERE);
			if (whereCause.size() > 1) {
				result.append(ALLOW_FILTERING);
			}
		}

		return result.toString();
	}

	private String generateWhereCauseById(HashMap<Field, Object> fieldsMap) {

		StringBuilder result;
		List<String> whereCause = new ArrayList<String>();

		for (Entry<Field, Object> set : fieldsMap.entrySet()) {
			Field field = set.getKey();
			Id id = field.getAnnotation(Id.class);

			if (Objects.isNull(id)) {
				continue;
			}
			Object value = set.getValue();
			value = prepareValue(value);
			if (!Objects.isNull(value)) {
				StringBuffer condition = new StringBuffer();
				Column column = field.getAnnotation(Column.class);
				String columnName = field.getName();
				if (!Objects.isNull(column)) {
					columnName = StringUtils.defaultString(column.name(),
							columnName);
				}
				condition.append(columnName).append(EQUAL_SYMBOL).append(value);
				whereCause.add(condition.toString());
			}
		}

		result = new StringBuilder(StringUtils.join(whereCause, AND));
		if (StringUtils.isNotEmpty(result.toString())) {
			result.insert(0, WHERE);
		}

		return result.toString();

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
			// TODO Skip key
			if (value == null || primaryColumn.contains(o)) {
				continue;
			}
			StringBuffer result = new StringBuffer(StringUtils.EMPTY);
			result.append(columnList.get(index)).append(EQUAL_SYMBOL)
					.append(value);
			set.add(result.toString());
		}
		return StringUtils.join(set, COMMA_SYMBOL);
	}

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
	 * @param type
	 * @param bean
	 * @param columnList
	 * @param paramList
	 * @return
	 */
	private HashMap<Field, Object> getFieldAndParamFromBean(METHOD_TYPE type,
			T bean, List<String> columnList, List<Object> paramList) {
		Field[] fields = bean.getClass().getDeclaredFields();
		HashMap<Field, Object> result = new HashMap<Field, Object>();

		for (Field f : fields) {
			Column column = f.getAnnotation(Column.class);
			if (column == null) {
				System.out.println("ERROR " + f.getName()
						+ "does not have column anotation");
				continue;
			}

			String columnName = f.getName();
			Object value = null;

			value = getValueFromClass(bean, columnName);
			value = prepareValue(value);

			if (METHOD_TYPE._SAVE == type) {
				Id id = f.getAnnotation(Id.class);
				if (!Objects.isNull(id)) {
					value = " now() ";
				}
			}

			columnList.add(column.name());
			paramList.add(value);
			result.put(f, value);
		}
		return result;
	}

	private void getFieldCreate(T bean, List<String> columns) {
		Field[] fields = bean.getClass().getDeclaredFields();
		for (Field f : fields) {
			Column column = f.getAnnotation(Column.class);
			if (column == null) {
				System.out.println("ERROR " + f.getName()
						+ "does not have column anotation");
				continue;
			}

			// ( pk int, t int, v text, s text static, PRIMARY KEY
			// (pk, t) )

			StringBuffer columnText = new StringBuffer(column.name());
			// TODO mapping type java classandra
			columnText.append(" ").append(f.getType());
			columns.add(column.name());
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

			Id id = f.getAnnotation(Id.class);
			if (!Objects.isNull(id)) {
				primaryColumn.add(key);
			}
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
