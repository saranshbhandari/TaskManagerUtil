StoredProcedureExecutor executor = StoredProcedureExecutorFactory.forType(s.getDatabaseType());
            StoredProcResult sp = executor.execute(ds, s);

            String taskPrefix = "Task" + task.getId() + ".";

            // Store OUT/INOUT (both SP name and alias already present in map)
            for (Map.Entry<String,Object> e : sp.getOutParams().entrySet()) {
                varstore.addVariable("${" + taskPrefix + e.getKey() + "}", e.getValue());
            }

            // Optional mirrors for your common names
            mirrorIfPresent(varstore, taskPrefix, sp.getOutParams(), "P_RESPONSEHEADER", "ResponseHeader");
            mirrorIfPresent(varstore, taskPrefix, sp.getOutParams(), "P_RESPONSEBODY",  "ResponseBody");
            mirrorIfPresent(varstore, taskPrefix, sp.getOutParams(), "P_RESPONSECODE",  "ResponseCode");

            // Store ResultSets
            List<List<Map<String,Object>>> rsList = sp.getResultSets();
            for (int i = 0; i < rsList.size(); i++) {
                varstore.addVariable("${" + taskPrefix + "ResultSet[" + i + "]}", rsList.get(i));
            }

            varstore.addVariable("${" + taskPrefix + "UpdateCount}", sp.getUpdateCountSum());

            log.info("{}Completed Successfully", logPrefix);
            return TaskStatus.Success;




// nnnnnndnsajfcnsdvfjnsdkvngsdksnvksnvklsdnvldksvnsdlkvsdvsd

    private void mirrorIfPresent(VarStore varstore, String taskPrefix, Map<String,Object> out,
                                 String spParamName, String canonical) {
        Object v = out.get(spParamName);
        if (v == null) v = out.get(spParamName.replaceFirst("^@", "")); // @Status → Status
        if (v != null) varstore.addVariable("${" + taskPrefix + canonical + "}", v);
    }
