package your.pkg.tasks.sp;

import javax.sql.DataSource;

public interface StoredProcedureExecutor {
    StoredProcResult execute(DataSource ds, ExecuteStoredProcedureTaskSettings settings) throws Exception;
}
