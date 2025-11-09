package your.pkg.tasks.sp;

import lombok.Data;
import java.util.*;

@Data
public class StoredProcResult {
    private final Map<String, Object> outParams = new LinkedHashMap<>();
    private final List<List<Map<String,Object>>> resultSets = new ArrayList<>();
    private int updateCountSum = 0;
}
