package cases;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.test.cassandra.bean.Employee;
import com.test.cassandra.dao.EmployeDao;

public class DeleteCqlTest {

	private Employee employee;
	private EmployeDao employeDao;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		employee = new Employee();
		employee.setName("Prajak");
		employeDao = new EmployeDao();
		List<Employee> data = employeDao.select(employee);
		Assert.assertNotNull(data);
		employee = data.get(0);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Ignore
	@Test
	public void deleteEmployeeWithoutWhereCause() throws Exception {
		employeDao.delete(employee);
	}

	@Test
	public void deleteEmployeeWithBean() throws Exception {
		employeDao.delete(employee);
		List<Employee> data = employeDao.select(employee);
		Assert.assertNull(data);
	}
}
