/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package pt.uminho.di.raft.entities.states;

import pt.uminho.di.raft.entities.*;
import org.apache.log4j.Logger;

/**
 *
 * @author fcampos
 */
public abstract class ServerStateTask {

    static Logger logger = Logger.getLogger(ServerStateTask.class);

    boolean running = true;
    Server server;

    public ServerStateTask(Server s)
    {
        server = s;
    }

    public boolean isRunning()
    {
        return running;
    }

    public void setRunning(boolean running)
    {
//        logger.debug(server.getIdString() + this.toString() + " Set running to " + running);
        this.running = running;
    }

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }
    
    public abstract void timeout();

    public void terminate()
    {
        setRunning(false);
    }

    public abstract void heartbeat();

    public abstract void execute();
}
