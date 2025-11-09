package your.pkg.tasks.sp;

public class StoredProcedureExecutorFactory {
    public static StoredProcedureExecutor forType(String databaseType) {
        if (databaseType == null) return new GenericStoredProcedureExecutor();
        switch (databaseType.trim().toLowerCase()) {
            case "oracle":    return new OracleStoredProcedureExecutor();
            case "mysql":     return new MySqlStoredProcedureExecutor();
            case "sqlserver": return new SqlServerStoredProcedureExecutor();
            default:          return new GenericStoredProcedureExecutor();
        }
    }
}
