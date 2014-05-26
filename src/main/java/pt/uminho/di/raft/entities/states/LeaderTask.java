/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pt.uminho.di.raft.entities.states;

import org.apache.log4j.Logger;
import org.ws4d.java.CoreFramework;
import pt.uminho.di.raft.entities.Server;

/**
 *
 * @author fcampos
 */
public class LeaderTask extends ServerStateTask {

    static Logger logger = Logger.getLogger(LeaderTask.class);

    public LeaderTask(Server srv) {
        super(srv);

        getServer().initializeLeaderVariables();
        
        // •	Upon election:
                //          send initial empty AppendEntries RPCs (heartbeat) to each server;
                //          repeat during idle periods to prevent election timeouts (§5.2)
        getServer().sendHeartbeatOrEntries(false);

        logger.debug(getServer().getIdString() + " LeaderTask has initialized!");
    }

    @Override
    public void execute()
    {
        getServer().sendHeartbeatOrEntries(true);
    }

    @Override
    public void timeout() {
        logger.debug(getServer().getIdString() + " Leader sending heartbeats or entries...");
        // •	Upon election:
                //          send initial empty AppendEntries RPCs (heartbeat) to each server;
                //          repeat during idle periods to prevent election timeouts (§5.2)
        execute();
    }

    @Override
    public void heartbeat() {
        logger.error(getServer().getIdString() + "Leader received heartbeat from another leader! Stepping down...");
        // turn into follower
        getServer().getTimeoutTask().setNextState(pt.uminho.di.raft.entities.states.State.Follower);
        getServer().getTimeoutTask().heartbeat();
    }
}
