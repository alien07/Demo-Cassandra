package com.dao.common;

import java.util.List;

public interface CommmonDAO<T> {

	void save(T t) throws Exception;

	void update(T t) throws Exception;

	void delete(T t) throws Exception;

	List<T> select(String query) throws Exception;
}
