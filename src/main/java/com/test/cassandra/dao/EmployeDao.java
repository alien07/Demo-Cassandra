package com.test.cassandra.dao;

import java.util.UUID;

import com.test.cassandra.bean.Employee;
import com.test.cassandra.dao.common.CassandraDao;

public class EmployeDao extends CassandraDao<Employee> {

	public String generate() {

		UUID dd = UUID.randomUUID();
		return dd.toString();
	}
}
