package cases;

import java.math.BigInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.test.cassandra.bean.Employee;
import com.test.cassandra.dao.EmployeDao;

public class SaveCqlTest {

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
		employeDao = new EmployeDao();
		employee = new Employee();
		employee.setName("Prajak");
		employee.setLastname("Manorin");
		employee.setCity("Bangkok");
		employee.setEmail("mprajak@test.com");
		employee.setPhone("0846466607");
		employee.setSalary(new BigInteger("50000"));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void saveEmployee() throws Exception {
		employeDao.save(employee);
	}

}
