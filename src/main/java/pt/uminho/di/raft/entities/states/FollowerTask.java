/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package pt.uminho.di.raft.entities.states;

import org.apache.log4j.Logger;
import pt.uminho.di.raft.entities.Server;

/**
 *
 * @author fcampos
 */
public class FollowerTask extends ServerStateTask {

    static Logger logger = Logger.getLogger(FollowerTask.class);

    public FollowerTask(Server srv)
    {
        super(srv);
    }

    @Override
    public void timeout()
    {
        // •	If election timeout elapses without receiving AppendEntries RPC
        // from current leader or granting vote to candidate: convert to candidate
          getServer().getTimeoutTask().setNextState(pt.uminho.di.raft.entities.states.State.Candidate);
          getServer().getTimeoutTask().notifyTask();
    }

    @Override
    public void heartbeat()
    {
        logger.debug(getServer().getIdString() + "Follower processing heartbeat...");
        getServer().getTimeoutTask().heartbeat();
    }

    @Override
    public void execute()
    {
        // do nothing
    }
}
