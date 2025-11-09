 package your.pkg.tasks.sp;

import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

@Slf4j
abstract class BaseStoredProcedureExecutor implements StoredProcedureExecutor {

    // ---------- type mapping ----------
    protected int toSqlType(String dbType, String dtype) {
        if (dtype == null) return Types.VARCHAR;
        String db = dbType == null ? "" : dbType.trim().toLowerCase(Locale.ROOT);
        String t  = dtype.trim().toUpperCase(Locale.ROOT);

        switch (t) {
            case "VARCHAR":
            case "VARCHAR2":
            case "NVARCHAR":
            case "NVARCHAR2": return Types.VARCHAR;
            case "CHAR":
            case "NCHAR":     return Types.CHAR;
            case "NUMBER":
            case "NUMERIC":
            case "DECIMAL":   return Types.NUMERIC;
            case "INT":
            case "INTEGER":   return Types.INTEGER;
            case "BIGINT":    return Types.BIGINT;
            case "FLOAT":     return Types.FLOAT;
            case "DOUBLE":    return Types.DOUBLE;
            case "BIT":
            case "BOOLEAN":   return Types.BOOLEAN;
            case "DATE":      return Types.DATE;
            case "DATETIME":
            case "TIMESTAMP": return Types.TIMESTAMP;
            case "CLOB":      return Types.CLOB;
            case "NCLOB":     return Types.NCLOB;
            case "BLOB":      return Types.BLOB;
            case "REF_CURSOR":
            case "CURSOR":
            case "SYS_REFCURSOR":
                if ("oracle".equals(db)) {
                    try {
                        return (int) Class.forName("oracle.jdbc.OracleTypes")
                                .getField("CURSOR").get(null);
                    } catch (Throwable ignore) {
                        log.debug("OracleTypes.CURSOR not found; falling back to Types.OTHER");
                        return Types.OTHER;
                    }
                }
                return Types.OTHER;
            default:
                return Types.VARCHAR;
        }
    }

    // ---------- SQL builder ----------
    protected String buildCallableSql(String dbType, String schema, String spname, int paramCount) {
        String qualified = (schema == null || schema.isBlank()) ? spname : (schema + "." + spname);
        String placeholders = paramCount == 0 ? "" :
                String.join(",", Collections.nCopies(paramCount, "?"));
        String sql = "{ call " + qualified + "(" + placeholders + ") }";
        log.debug("Callable SQL built for {}: {}", dbType, sql);
        return sql;
    }

    // ---------- parameter binding ----------
    protected void bindAndRegisterParams(CallableStatement cs, ExecuteStoredProcedureTaskSettings s) throws Exception {
        if (s.getParams() == null) return;
        String dbType = s.getDatabaseType();

        for (int i = 0; i < s.getParams().size(); i++) {
            int idx = i + 1;
            var p = s.getParams().get(i);
            String dir = p.getType() == null ? "IN" : p.getType().trim().toUpperCase(Locale.ROOT);
            int sqlType = toSqlType(dbType, p.getDatatype());

            switch (dir) {
                case "IN":
                    log.debug("Bind IN  idx={} name={} type={} val={}", idx, p.getParameter(), sqlType, p.getValue());
                    if (p.getValue() == null) cs.setNull(idx, sqlType);
                    else cs.setObject(idx, p.getValue());
                    break;
                case "OUT":
                    log.debug("Reg  OUT idx={} name={} type={}", idx, p.getParameter(), sqlType);
                    cs.registerOutParameter(idx, sqlType);
                    break;
                case "INOUT":
                    log.debug("Bind INOUT idx={} name={} type={} val={} [register OUT too]", idx, p.getParameter(), sqlType, p.getValue());
                    if (p.getValue() == null) cs.setNull(idx, sqlType);
                    else cs.setObject(idx, p.getValue());
                    cs.registerOutParameter(idx, sqlType);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown param type: " + p.getType());
            }
        }
    }

    // ---------- resultset util ----------
    protected List<Map<String,Object>> rsToList(ResultSet rs) throws SQLException {
        List<Map<String,Object>> rows = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();
        while (rs.next()) {
            Map<String,Object> row = new LinkedHashMap<>();
            for (int i=1;i<=cols;i++) {
                String col = md.getColumnLabel(i);
                if (col == null || col.isEmpty()) col = md.getColumnName(i);
                row.put(col, rs.getObject(i));
            }
            rows.add(row);
        }
        log.debug("Converted ResultSet -> {} row(s)", rows.size());
        return rows;
    }

    // ---------- OUT collection ----------
    protected void collectOutParams(CallableStatement cs, ExecuteStoredProcedureTaskSettings s,
                                    StoredProcResult result) throws SQLException {
        if (s.getParams() == null) return;

        for (int i = 0; i < s.getParams().size(); i++) {
            var p = s.getParams().get(i);
            String dir = p.getType() == null ? "IN" : p.getType().trim().toUpperCase(Locale.ROOT);
            if ("OUT".equals(dir) || "INOUT".equals(dir)) {
                Object val = cs.getObject(i + 1);
                String baseKey = normalizeName(p.getParameter());
                result.getOutParams().put(baseKey, val);
                if (p.getValue() != null && !p.getValue().isBlank()) {
                    result.getOutParams().put(p.getValue(), val);   // alias (your "test" requirement)
                }
                log.debug("OUT collected name={} alias={} -> {}", baseKey, p.getValue(), val);
            }
        }
    }

    protected String normalizeName(String n) {
        return (n == null) ? "" : n.replaceFirst("^@", "");
    }

    // ---------- main execute ----------
    @Override
    public StoredProcResult execute(DataSource ds, ExecuteStoredProcedureTaskSettings s) throws Exception {
        StoredProcResult result = new StoredProcResult();
        String sql = buildCallableSql(s.getDatabaseType(), s.getSchemaName(), s.getSpname(),
                                      s.getParams() == null ? 0 : s.getParams().size());

        try (Connection con = ds.getConnection();
             CallableStatement cs = con.prepareCall(sql)) {

            log.info("SP start dbType={} schema={} sp={} params={}",
                    s.getDatabaseType(), s.getSchemaName(), s.getSpname(),
                    (s.getParams()==null?0:s.getParams().size()));

            bindAndRegisterParams(cs, s);

            boolean hasResults = cs.execute();
            int updateCount = cs.getUpdateCount();
            int rsIdx = 0;

            while (true) {
                if (hasResults) {
                    try (ResultSet rs = cs.getResultSet()) {
                        List<Map<String, Object>> list = rsToList(rs);
                        result.getResultSets().add(list);
                        log.debug("Captured ResultSet[{}] rows={}", rsIdx++, list.size());
                    }
                } else if (updateCount != -1) {
                    result.setUpdateCountSum(result.getUpdateCountSum() + updateCount);
                    log.debug("DML updateCount += {}", updateCount);
                } else {
                    break;
                }
                hasResults = cs.getMoreResults();
                updateCount = cs.getUpdateCount();
            }

            collectOutParams(cs, s, result);
            afterExecute(cs, s, result); // DB-specific hook

            log.info("SP done dbType={} sp={} totalUpdateCount={} rsCount={} outCount={}",
                    s.getDatabaseType(), s.getSpname(),
                    result.getUpdateCountSum(),
                    result.getResultSets().size(),
                    result.getOutParams().size());
        }
        return result;
    }

    // DB-specific tweaks (default: no-op)
    protected void afterExecute(CallableStatement cs, ExecuteStoredProcedureTaskSettings s,
                                StoredProcResult result) throws Exception { }
}
