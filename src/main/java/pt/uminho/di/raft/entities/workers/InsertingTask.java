/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package pt.uminho.di.raft.entities.workers;

import org.apache.log4j.Logger;
import org.ws4d.java.types.EndpointReference;
import pt.uminho.di.raft.entities.Server;
import pt.uminho.di.raft.entities.ServerClient;

/**
 *
 * @author fcampos
 */
public class InsertingTask extends ServerWorkingTask {

    static Logger logger = Logger.getLogger(InsertingTask.class);

    private ServerClient serverClient;
    private EndpointReference epr;

    public InsertingTask(ServerClient cli, Server srv, EndpointReference e)
    {
        super(srv);
        serverClient = cli;
        epr = e;
    }

    @Override
    public void run()
    {
        logger.debug(getServer().getIdString() + "Inserting Task started for EPR:" + epr);
        serverClient.insertServer(epr);
        logger.debug(getServer().getIdString() + "Inserting Task ended.");
    }
}
