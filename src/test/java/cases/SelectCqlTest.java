package cases;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.test.cassandra.bean.Employee;
import com.test.cassandra.dao.EmployeDao;

public class SelectCqlTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void selectEmployeeWithoutWhereCause() throws Exception {
		String query = "select * from emp";
		EmployeDao employeDao = new EmployeDao();

		List<Employee> data = employeDao.select(query);
		Assert.assertNotNull(data);
	}

	@Test
	public void selectEmployeeWithWhereCause() throws Exception {
		String query = "select * from emp where emp_name ='Prajak'";
		EmployeDao employeDao = new EmployeDao();

		List<Employee> data = employeDao.select(query);
		Assert.assertNotNull(data);
	}

	@Test
	public void selectEmployeeWithEmptyBean() throws Exception {
		EmployeDao employeDao = new EmployeDao();
		Employee emp = new Employee();
		List<Employee> data = employeDao.select(emp);
		Assert.assertNotNull(data);
	}

	@Test
	public void selectEmployeeWithBean() throws Exception {
		EmployeDao employeDao = new EmployeDao();
		Employee emp = new Employee();
		emp.setName("Prajak");
		List<Employee> data = employeDao.select(emp);
		Assert.assertNotNull(data);
	}

}
