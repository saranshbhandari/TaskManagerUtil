import java.util.List;
import lombok.Data;

@Data
public class IFTaskSettings {
    private String betweenGroupsOperator; // "AND" or "OR"
    private List<Group> groups;

    @Data
    public static class Group {
        private String id;
        private String groupOperator; // "AND" or "OR"
        private List<Condition> conditions;
    }

    @Data
    public static class Condition {
        private String firstValue;
        private String operator;   // "equals", "notequal", "isnull", etc.
        private String secondValue; // optional
    }

    // -------------------------
    // Evaluation Logic
    // -------------------------
    public boolean evaluate() {
        if (groups == null || groups.isEmpty()) return true;

        boolean result = "AND".equalsIgnoreCase(betweenGroupsOperator);

        for (Group group : groups) {
            boolean groupResult = evaluateGroup(group);

            if ("AND".equalsIgnoreCase(betweenGroupsOperator)) {
                result = result && groupResult;
            } else {
                result = result || groupResult;
            }
        }

        return result;
    }

    private boolean evaluateGroup(Group group) {
        if (group.getConditions() == null || group.getConditions().isEmpty()) return true;

        boolean groupResult = "AND".equalsIgnoreCase(group.getGroupOperator());

        for (Condition condition : group.getConditions()) {
            boolean condResult = evaluateCondition(condition);

            if ("AND".equalsIgnoreCase(group.getGroupOperator())) {
                groupResult = groupResult && condResult;
            } else {
                groupResult = groupResult || condResult;
            }
        }

        return groupResult;
    }

    private boolean evaluateCondition(Condition c) {
        String op = c.getOperator();
        String left = c.getFirstValue();
        String right = c.getSecondValue();

        return switch (op.toLowerCase()) {
            case "equals" -> equalsCheck(left, right);
            case "notequal" -> !equalsCheck(left, right);
            case "isnull" -> left == null;
            case "isnotnull" -> left != null;
            case "isemptystring" -> left == null || left.isEmpty();
            case "isnotemptystring" -> left != null && !left.isEmpty();
            default -> false;
        };
    }

    private boolean equalsCheck(String left, String right) {
        if (left == null && right == null) return true;
        if (left == null || right == null) return false;
        try {
            double l = Double.parseDouble(left);
            double r = Double.parseDouble(right);
            return l == r;
        } catch (NumberFormatException e) {
            return left.equals(right);
        }
    }
}
