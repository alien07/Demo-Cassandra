package cases;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.test.cassandra.bean.Employee;
import com.test.cassandra.dao.EmployeDao;

public class UpdateCqlTest {

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
		String query = "select * from emp where emp_name = 'Prajak'";
		employeDao = new EmployeDao();
		List<Employee> data = employeDao.select(query);
		employee = data.get(0);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void updateEmployeeById() throws Exception {
		employee.setEmail("after_update@test.com");
		employeDao.update(employee);
	}

}
