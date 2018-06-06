package org.kramerius.replications;

import static org.kramerius.replications.K4ReplicationProcess.*;

import cz.incad.kramerius.processes.annotations.ParameterName;
import cz.incad.kramerius.processes.annotations.Process;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;



public class FedoraDataMigrationProcess {

    public static final Logger LOGGER = Logger.getLogger(FedoraDataMigrationProcess.class.getName());

    public static Phase[] PHASES = new Phase[] {
            new ZeroPhase(),
            new IterateThroughIndexPhase(),
            new SecondPhase(),
            new StartIndexerPhase()
    };


    @Process
    public static void replications(@ParameterName("url") String url,
                                    @ParameterName("username") String userName,
                                    @ParameterName("pswd")String pswd,
                                    @ParameterName("replicateCollections")String replicateCollections,
                                    @ParameterName("replicateImages")String replicateImages,
                                    @ParameterName("previousProcess")String previousProcessUUID) throws IOException {
        start(url, userName, pswd, replicateCollections, replicateImages, PHASES);
    }
}
