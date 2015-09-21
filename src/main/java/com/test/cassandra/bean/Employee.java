package com.test.cassandra.bean;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

@Entity
@Table(name = "emp", indexes = { @Index(columnList = "emp_name"),
		@Index(columnList = "emp_last_name"), @Index(columnList = "emp_city"),
		@Index(columnList = "emp_salary") })
public class Employee implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1555141932595663642L;

	/**
	 * 
	 */

	@Id
	@Column(name = "emp_id")
	UUID id;

	@Column(name = "emp_city")
	String city;

	@Column(name = "emp_email")
	String email;

	@Column(name = "emp_last_name")
	String lastname;

	@Column(name = "emp_name")
	String name;

	@Column(name = "emp_phone", length = 12)
	String phone;

	@Column(name = "emp_salary", scale = 13, precision = 2)
	BigInteger salary;

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public BigInteger getSalary() {
		return salary;
	}

	public void setSalary(BigInteger salary) {
		this.salary = salary;
	}

}
