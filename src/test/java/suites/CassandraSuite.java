package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import cases.DeleteCqlTest;
import cases.SaveCqlTest;
import cases.SelectCqlTest;
import cases.UpdateCqlTest;


@RunWith(Suite.class)
@SuiteClasses({ SelectCqlTest.class, SaveCqlTest.class, UpdateCqlTest.class, DeleteCqlTest.class})
public class CassandraSuite {

}
