package cz.incad.kramerius.security.licenses.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import cz.incad.kramerius.SolrAccess;
import cz.incad.kramerius.security.licenses.License;
import cz.incad.kramerius.security.licenses.LicensesManager;
import cz.incad.kramerius.security.licenses.LicensesManagerException;
import cz.incad.kramerius.utils.StringUtils;
import cz.incad.kramerius.utils.conf.KConfiguration;
import cz.incad.kramerius.utils.database.JDBCCommand;
import cz.incad.kramerius.utils.database.JDBCQueryTemplate;
import cz.incad.kramerius.utils.database.JDBCTransactionTemplate;

public class DatabaseLicensesManagerImpl implements LicensesManager {

    /** Must correspond with register.digitalniknihovna.cz */
    public static final String ACRONYM_LIBRARY_KEY = "acronym";

    private Provider<Connection> provider;

    private SolrAccess solrAccess;

    private String acronym;

    @Inject
    public DatabaseLicensesManagerImpl(@Named("kramerius4") Provider<Connection> provider,
            @Named("new-index") SolrAccess solrAccess) {
        this.provider = provider;
        this.solrAccess = solrAccess;
        this.acronym = KConfiguration.getInstance().getConfiguration().getString(ACRONYM_LIBRARY_KEY, "");
    }

    @Override
    public int getMinPriority() throws LicensesManagerException {
        List<Integer> priorities = new JDBCQueryTemplate<Integer>(this.provider.get(), true) {
            @Override
            public boolean handleRow(ResultSet rs, List<Integer> returnsList) throws SQLException {
                int max_priority = rs.getInt("label_priority");
                returnsList.add(max_priority);
                return super.handleRow(rs, returnsList);
            }
        }.executeQuery("select max(label_priority) label_priority from labels_entity");
        return priorities.isEmpty() ? License.DEFAULT_PRIORITY : priorities.get(0);
    }

    @Override
    public int getMaxPriority() throws LicensesManagerException {
        return License.DEFAULT_PRIORITY;
    }

    @Override
    public void addLocalLicense(License license) throws LicensesManagerException {
        check(license);

        Connection connection = null;
        int transactionIsolation = -1;
        try {
            connection = provider.get();
            transactionIsolation = connection.getTransactionIsolation();

            new JDBCTransactionTemplate(connection, true).updateWithTransaction(new JDBCCommand() {
                @Override
                public Object executeJDBCCommand(Connection con) throws SQLException {
                    List<Integer> priorities = new JDBCQueryTemplate<Integer>(con, false) {
                        @Override
                        public boolean handleRow(ResultSet rs, List<Integer> returnsList) throws SQLException {
                            int max_priority = rs.getInt("label_priority");
                            returnsList.add(max_priority);
                            return super.handleRow(rs, returnsList);
                        }
                    }.executeQuery("select max(label_priority) label_priority from labels_entity");
                    return priorities;
                }
            }, new JDBCCommand() {
                @Override
                public Object executeJDBCCommand(Connection con) throws SQLException {
                    List<Integer> priorities = (List<Integer>) getPreviousResult();
                    Integer min = priorities.isEmpty() ? License.DEFAULT_PRIORITY : Collections.max(priorities) + 1;
                    // id, group, name, description, priority
                    PreparedStatement prepareStatement = con.prepareStatement(
                            "insert into labels_entity(label_id,label_group,label_name, label_description, label_priority) values(nextval('LABEL_ID_SEQUENCE'), ?, ?, ?, ?)");
                    prepareStatement.setString(1, license.getGroup());
                    prepareStatement.setString(2, license.getName());
                    prepareStatement.setString(3, license.getDescription());
                    prepareStatement.setInt(4, min);

                    return prepareStatement.executeUpdate();

                }
            });
        } catch (SQLException e) {
            throw new LicensesManagerException(e.getMessage(), e);
        }
    }

    private void check(License license) throws LicensesManagerException {
        checkAcronym();
        if (!license.getGroup().equals(LicensesManager.LOCAL_GROUP_NAME)) {
            throw new LicensesManagerException("Given license is not local license");
        }

        if (!license.getName().startsWith(this.acronym + "_")) {
            throw new LicensesManagerException("Local license must start with '" + this.acronym + "_'");
        }
    }

    private void checkAcronym() throws LicensesManagerException {
        if (!StringUtils.isAnyString(this.acronym)) {
            throw new LicensesManagerException("property acronym must be defined in configuration.properties");
        }
    }

    @Override
    public void removeLocalLicense(License license) throws LicensesManagerException {
        check(license);
        try {

            new JDBCTransactionTemplate(provider.get(), true).updateWithTransaction(

                    new JDBCCommand() {
                        @Override
                        public Object executeJDBCCommand(Connection con) throws SQLException {
                            PreparedStatement prepareStatement = con
                                    .prepareStatement("delete from rights_criterium_entity where label_id = ?");
                            prepareStatement.setInt(1, license.getId());
                            return prepareStatement.executeUpdate();
                        }
                    },

                    new JDBCCommand() {
                        @Override
                        public Object executeJDBCCommand(Connection con) throws SQLException {
                            PreparedStatement prepareStatement = con
                                    .prepareStatement("delete from labels_entity where label_id = ?");
                            prepareStatement.setInt(1, license.getId());
                            return prepareStatement.executeUpdate();
                        }
                    }, new JDBCCommand() {
                        @Override
                        public Object executeJDBCCommand(Connection con) throws SQLException {
                            PreparedStatement prepareStatement = con.prepareStatement(
                                    "update labels_entity set label_priority = label_priority-1 where label_priority > ?",
                                    license.getPriority());
                            prepareStatement.setInt(1, license.getPriority());
                            return prepareStatement.executeUpdate();
                        }
                    }

            );
        } catch (SQLException e) {
            throw new LicensesManagerException(e.getMessage(), e);
        }
    }

    
    
    
    
    
    @Override
    public void rearrangePriorities(List<License> globalLicenses, Connection conn) throws LicensesManagerException {
        globalLicenses.sort((left, right) -> {
            int leftHint = left.getPriorityHint();
            int rightHint = right.getPriorityHint();
            return Integer.compare(leftHint, rightHint);
        });
        
        
        String condition = globalLicenses.stream().map( lic -> {
            return "label_name='"+lic.getName()+"'";
        }).collect(Collectors.joining(" OR "));
        

        List<License> realLicenses =new JDBCQueryTemplate<License>(conn, false) {
            @Override
            public boolean handleRow(ResultSet rs, List<License> returnsList) throws SQLException {
                returnsList.add(createLabelFromResultSet(rs));
                return super.handleRow(rs, returnsList);
            }
        }.executeQuery(String.format("select * from labels_entity where  %s order by label_priority asc", condition));
        

        // real priorities
        Map<String, License> realLienseMappings = new HashMap<>();
        realLicenses.forEach(lic-> {realLienseMappings.put(lic.getName(), lic); });
        
        List<JDBCCommand> commands = new ArrayList<>();
        for (int i = 0; i < globalLicenses.size(); i++) {
            License globalLicense = globalLicenses.get(i);
            int realPriority = realLicenses.get(i).getPriority();
            License realLicense = realLienseMappings.get(globalLicense.getName());
            commands.add(new  UpdatePriorityCommand(realLicense.getUpdatedPriorityLabel(realPriority)));
        }

        try {
            new JDBCTransactionTemplate(conn, false).updateWithTransaction(commands);
        } catch (SQLException e) {
            throw new LicensesManagerException(e.getMessage(), e);
        }
        
    }

    @Override
    public void rearrangePriorities(List<License> globalLicenses) throws LicensesManagerException {

        globalLicenses.sort((left, right) -> {
            int leftHint = left.getPriorityHint();
            int rightHint = right.getPriorityHint();
            return Integer.compare(leftHint, rightHint);
        });
        
        
        String condition = globalLicenses.stream().map( lic -> {
            return "label_name='"+lic.getName()+"'";
        }).collect(Collectors.joining(" OR "));
        

        List<License> realLicenses =new JDBCQueryTemplate<License>(provider.get()) {
            @Override
            public boolean handleRow(ResultSet rs, List<License> returnsList) throws SQLException {
                returnsList.add(createLabelFromResultSet(rs));
                return super.handleRow(rs, returnsList);
            }
        }.executeQuery(String.format("select * from labels_entity where  %s order by label_priority asc", condition));
        

        // real priorities
        Map<String, License> realLienseMappings = new HashMap<>();
        realLicenses.forEach(lic-> {realLienseMappings.put(lic.getName(), lic); });
        
        List<JDBCCommand> commands = new ArrayList<>();
        for (int i = 0; i < globalLicenses.size(); i++) {
            License globalLicense = globalLicenses.get(i);
            int realPriority = realLicenses.get(i).getPriority();
            License realLicense = realLienseMappings.get(globalLicense.getName());
            commands.add(new  UpdatePriorityCommand(realLicense.getUpdatedPriorityLabel(realPriority)));
        }

        try {
            new JDBCTransactionTemplate(provider.get(), true).updateWithTransaction(commands);
        } catch (SQLException e) {
            throw new LicensesManagerException(e.getMessage(), e);
        }

    }

    @Override
    public License getLicenseByPriority(int priority) throws LicensesManagerException {
        List<License> licenses = new JDBCQueryTemplate<License>(provider.get()) {
            @Override
            public boolean handleRow(ResultSet rs, List<License> returnsList) throws SQLException {
                returnsList.add(createLabelFromResultSet(rs));
                return super.handleRow(rs, returnsList);
            }
        }.executeQuery("select * from labels_entity where LABEL_PRIORITY = ? ", priority);
        return licenses.isEmpty() ? null : licenses.get(0);
    }

    @Override
    public License getLicenseById(int id) throws LicensesManagerException {
        List<License> licenses = new JDBCQueryTemplate<License>(provider.get()) {
            @Override
            public boolean handleRow(ResultSet rs, List<License> returnsList) throws SQLException {
                returnsList.add(createLabelFromResultSet(rs));
                return super.handleRow(rs, returnsList);
            }
        }.executeQuery("select * from labels_entity where LABEL_ID = ? ", id);
        return licenses.isEmpty() ? null : licenses.get(0);
    }

    @Override
    public License getLicenseByName(String name) throws LicensesManagerException {
        List<License> licenses = new JDBCQueryTemplate<License>(provider.get()) {
            @Override
            public boolean handleRow(ResultSet rs, List<License> returnsList) throws SQLException {
                returnsList.add(createLabelFromResultSet(rs));
                return super.handleRow(rs, returnsList);
            }
        }.executeQuery("select * from labels_entity where LABEL_NAME = ? ", name);
        return licenses.isEmpty() ? null : licenses.get(0);
    }

    @Override
    public void updateLocalLicense(License license) throws LicensesManagerException {
        try {
            new JDBCTransactionTemplate(provider.get(), true).updateWithTransaction(new JDBCCommand() {
                @Override
                public Object executeJDBCCommand(Connection con) throws SQLException {
                    PreparedStatement prepareStatement = con.prepareStatement(
                            "update labels_entity  " + "set LABEL_NAME=? , LABEL_DESCRIPTION=? where label_id = ?");

                    prepareStatement.setString(1, license.getName());
                    prepareStatement.setString(2, license.getDescription());

                    prepareStatement.setInt(3, license.getId());

                    return prepareStatement.executeUpdate();

                }
            });
        } catch (SQLException e) {
            throw new LicensesManagerException(e.getMessage(), e);
        }
    }

    @Override
    public void moveUp(License license) throws LicensesManagerException {
        //check(license);
        int priority = license.getPriority();
        if (priority >= 2) {
            License upPriorityLicense = getLicenseByPriority(priority - 1);
            if (upPriorityLicense != null) {
                int upPriority = upPriorityLicense.getPriority();
                try {
                    new JDBCTransactionTemplate(provider.get(), true).updateWithTransaction(
                            new UpdatePriorityCommand(license.getUpdatedPriorityLabel(-1)),
                            new UpdatePriorityCommand(upPriorityLicense.getUpdatedPriorityLabel(-1)),

                            new UpdatePriorityCommand(license.getUpdatedPriorityLabel(upPriority)),
                            new UpdatePriorityCommand(upPriorityLicense.getUpdatedPriorityLabel(priority)));
                } catch (SQLException e) {
                    throw new LicensesManagerException(e.getMessage(), e);
                }
            }
        } else
            throw new LicensesManagerException("cannot increase the priority for " + license);
    }

    @Override
    public void moveDown(License license) throws LicensesManagerException {
        //check(license);
        int priority = license.getPriority();
        if (priority < getMinPriority()) {
            License downPriorityLicense = getLicenseByPriority(priority + 1);
            if (downPriorityLicense != null) {
                int downPriority = downPriorityLicense.getPriority();

                try {
                    new JDBCTransactionTemplate(provider.get(), true).updateWithTransaction(
                            new UpdatePriorityCommand(license.getUpdatedPriorityLabel(-1)),
                            new UpdatePriorityCommand(downPriorityLicense.getUpdatedPriorityLabel(-1)),

                            new UpdatePriorityCommand(license.getUpdatedPriorityLabel(downPriority)),
                            new UpdatePriorityCommand(downPriorityLicense.getUpdatedPriorityLabel(priority)));
                } catch (SQLException e) {
                    throw new LicensesManagerException(e.getMessage(), e);
                }
            }
        } else
            throw new LicensesManagerException("cannot decrease the priority for " + license);
    }

    @Override
    public List<License> getLicenses() {
        return new JDBCQueryTemplate<License>(provider.get()) {
            @Override
            public boolean handleRow(ResultSet rs, List<License> returnsList) throws SQLException {
                returnsList.add(createLabelFromResultSet(rs));
                return super.handleRow(rs, returnsList);
            }
        }.executeQuery("select * from labels_entity order by LABEL_PRIORITY ASC NULLS LAST ");
    }

    private License createLabelFromResultSet(ResultSet rs) throws SQLException {
        int labelId = rs.getInt("label_id");
        String name = rs.getString("LABEL_NAME");
        if (name == null)
            name = "";
        String groupName = rs.getString("LABEL_GROUP");
        String description = rs.getString("LABEL_DESCRIPTION");
        int priority = rs.getInt("LABEL_PRIORITY");
        return new LicenseImpl(labelId, name, description, groupName, priority);
    }

    @Override
    public void refreshLabelsFromSolr() throws LicensesManagerException {
        throw new UnsupportedOperationException("unsupported");
//        try {
//            Document request = this.solrAccess.requestWithSelectReturningXml("facet.field=licenses&fl=licenses&q=*%3A*&rows=0&facet=on");
//            Element dnntLabelsFromSolr = XMLUtils.findElement(request.getDocumentElement(), new XMLUtils.ElementsFilter() {
//                @Override
//                public boolean acceptElement(Element element) {
//                    String name = element.getAttribute("name");
//                    return name != null && name.equals("licenses");
//                }
//            });
//            List<String> labelsUsedInSolr = XMLUtils.getElements(dnntLabelsFromSolr).stream().map(element -> {
//                return element.getAttribute("name");
//            }).collect(Collectors.toList());
//
//            getLicenses().stream().forEach(eLabel-> {
//                labelsUsedInSolr.remove(eLabel.getName());
//            });
//
//            for (String lname : labelsUsedInSolr) {  this.addLocalLicense(new LicenseImpl(lname, "", LicensesManager.GLOBAL_GROUP_NAME)); }
//
//        } catch (IOException e) {
//            throw new LicensesManagerException(e.getMessage(),e);
//        }

    }

    
    private static class UpdatePriorityCommand extends JDBCCommand {

        private License license;

        public UpdatePriorityCommand(License license) {
            this.license = license;
        }

        @Override
        public Object executeJDBCCommand(Connection con) throws SQLException {
            PreparedStatement prepareStatement = con
                    .prepareStatement("update labels_entity set label_priority = ? where label_id = ? ");

            if (license.getPriority() == -1) {
                prepareStatement.setNull(1, Types.INTEGER);
            } else {
                prepareStatement.setInt(1, license.getPriority());
            }
            prepareStatement.setInt(2, license.getId());

            return prepareStatement.executeUpdate();
        }
    }

    @Override
    public List<License> getGlobalLicenses() throws LicensesManagerException {
        return new JDBCQueryTemplate<License>(provider.get()) {
            @Override
            public boolean handleRow(ResultSet rs, List<License> returnsList) throws SQLException {
                returnsList.add(createLabelFromResultSet(rs));
                return super.handleRow(rs, returnsList);
            }
        }.executeQuery(
                "select * from labels_entity where label_group = ? OR label_group = ? order by LABEL_PRIORITY ASC NULLS LAST ",
                LicensesManager.GLOBAL_GROUP_NAME_IMPORTED, LicensesManager.GLOBAL_GROUP_NAME_EMBEDDED);
    }

    @Override
    public List<License> getLocalLicenses() throws LicensesManagerException {
        return new JDBCQueryTemplate<License>(provider.get()) {
            @Override
            public boolean handleRow(ResultSet rs, List<License> returnsList) throws SQLException {
                returnsList.add(createLabelFromResultSet(rs));
                return super.handleRow(rs, returnsList);
            }
        }.executeQuery("select * from labels_entity where label_group = ? order by LABEL_PRIORITY ASC NULLS LAST  ",
                LicensesManager.LOCAL_GROUP_NAME);
    }

    @Override
    public List<License> getAllLicenses() throws LicensesManagerException {
        return new JDBCQueryTemplate<License>(provider.get()) {
            @Override
            public boolean handleRow(ResultSet rs, List<License> returnsList) throws SQLException {
                returnsList.add(createLabelFromResultSet(rs));
                return super.handleRow(rs, returnsList);
            }
        }.executeQuery("select * from labels_entity order by LABEL_PRIORITY ASC NULLS LAST ");
    }

}
