/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package pt.uminho.di.raft.entities.workers;

import org.apache.log4j.Logger;
import pt.uminho.di.raft.entities.Server;
import pt.uminho.di.raft.entities.ServerClient;

/**
 *
 * @author fcampos
 */
public class SearchingTask extends ServerWorkingTask {

    static Logger logger = Logger.getLogger(SearchingTask.class);

    private ServerClient serverClient;

    public SearchingTask(ServerClient cli, Server srv)
    {
        super(srv);
        serverClient = cli;
    }

    @Override
    public void run()
    {
        serverClient.searchRaftDevices();
    }
}
