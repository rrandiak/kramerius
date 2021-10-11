package cz.incad.kramerius.solr;

public abstract class SolrFieldsMapping {

    // Generic fields
    public abstract String getPidField();
    public abstract String getPidPathField();

    // Labels Fields
    public abstract String getDnntFlagField();
    public abstract String getDnntLabelsField();
    public abstract String getContainsDnntLabelsField();

    public static SolrFieldsMapping getInstance() {
        return new SolrFieldsMapping.K5FieldsMapping();
    }

    private static class K5FieldsMapping extends SolrFieldsMapping {

        public static final String PID_IDENTIFIER = "PID";
        public static final String PID_PATH_FIELD = "pid_path";
        public static final String DNNT_FLAG_FIELD = "dnnt";
        public static final String DNNT_LABELS_FIELD = "dnnt-labels";
        public static final String DNNT_CONTAINS_LABELS_FIELD = "contains-dnnt-labels";

        @Override
        public String getPidField() {
            return PID_IDENTIFIER;
        }

        @Override
        public String getPidPathField() {
            return PID_PATH_FIELD;
        }

        @Override
        public String getDnntLabelsField() {
            return DNNT_LABELS_FIELD;
        }

        @Override
        public String getContainsDnntLabelsField() {
            return DNNT_CONTAINS_LABELS_FIELD;
        }

        @Override
        public String getDnntFlagField() {
            return DNNT_FLAG_FIELD;
        }
    }


    private static class K7FieldsMapping extends  SolrFieldsMapping {

        public static final String PID_IDENTIFIER = "pid";
        public static final String PID_PATH_FIELD = "own_pid_path";
        public static final String DNNT_LABELS_FIELD = "dnnt-labels";
        public static final String DNNT_CONTAINS_LABELS_FIELD = "contains-dnnt-labels";

        @Override
        public String getPidField() {
            return PID_IDENTIFIER;
        }

        @Override
        public String getPidPathField() {
            return PID_PATH_FIELD;
        }

        @Override
        public String getDnntLabelsField() {
            return DNNT_LABELS_FIELD;
        }

        @Override
        public String getContainsDnntLabelsField() {
            return DNNT_CONTAINS_LABELS_FIELD;
        }

        @Override
        public String getDnntFlagField() {
            return null;
        }
    }
}
