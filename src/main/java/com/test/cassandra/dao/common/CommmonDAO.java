package com.test.cassandra.dao.common;

import java.util.List;

public interface CommmonDAO<T> {

	List<T> select(T bean) throws Exception;

	List<T> select(String query) throws Exception;

	void save(T t) throws Exception;

	void save(String query) throws Exception;

	void update(T t) throws Exception;

	void update(String query) throws Exception;

	void delete(T t) throws Exception;

	void delete(String query) throws Exception;

}
