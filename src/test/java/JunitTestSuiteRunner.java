import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import suites.CassandraSuite;
import utils.MessageUtil;

/**
 * 
 */

/**
 * @author mprajak
 *
 */
public class JunitTestSuiteRunner {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Result result = JUnitCore.runClasses(CassandraSuite.class);
		MessageUtil.printResult(result);		
	}

//	@SuppressWarnings("unused")
//	private static void testJson(String stringJson){		
//		List<Map<String, Object>> data = JsonUtil.toList(stringJson);
//		System.out.println("data:"+  data);
//		
//	}
	
}
