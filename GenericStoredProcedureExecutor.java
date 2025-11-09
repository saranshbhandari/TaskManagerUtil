package your.pkg.tasks.sp;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class GenericStoredProcedureExecutor extends BaseStoredProcedureExecutor {
    @Override
    protected void afterExecute(java.sql.CallableStatement cs,
                                ExecuteStoredProcedureTaskSettings s,
                                StoredProcResult result) {
        log.debug("Generic executor afterExecute hook.");
    }
}


package your.pkg.tasks.sp;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class OracleStoredProcedureExecutor extends BaseStoredProcedureExecutor {
    @Override
    protected void afterExecute(java.sql.CallableStatement cs,
                                ExecuteStoredProcedureTaskSettings s,
                                StoredProcResult result) {
        log.debug("Oracle afterExecute hook (REF_CURSOR handled in base).");
    }
}



package your.pkg.tasks.sp;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class MySqlStoredProcedureExecutor extends BaseStoredProcedureExecutor {
    @Override
    protected void afterExecute(java.sql.CallableStatement cs,
                                ExecuteStoredProcedureTaskSettings s,
                                StoredProcResult result) {
        log.debug("MySQL afterExecute hook (multiple RS handled in base).");
    }
}

package your.pkg.tasks.sp;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class SqlServerStoredProcedureExecutor extends BaseStoredProcedureExecutor {
    @Override
    protected void afterExecute(java.sql.CallableStatement cs,
                                ExecuteStoredProcedureTaskSettings s,
                                StoredProcResult result) {
        log.debug("SQL Server afterExecute hook.");
    }
}
