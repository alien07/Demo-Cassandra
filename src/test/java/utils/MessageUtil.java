/**
 * 
 */
package utils;

import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * @author mprajak
 *
 */
public class MessageUtil {
	public static void printResult(Result result) {
		if (result == null) {
			return;
		}
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.toString());
		}
		if (result.wasSuccessful()) {
			System.out.println("All tests finished successfully...");
		}

	}
}
