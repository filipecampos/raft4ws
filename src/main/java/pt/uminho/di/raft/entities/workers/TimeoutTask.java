/*******************************************************************************
 * Copyright (c) 2014 Filipe Campos.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package pt.uminho.di.raft.entities.workers;

import org.apache.log4j.Logger;
import pt.uminho.di.raft.entities.Server;

public class TimeoutTask extends ServerWorkingTask {

    static Logger logger = Logger.getLogger(TimeoutTask.class);
    long timeout_period;
    boolean timeout = false;
    boolean terminated = false;
    boolean contacted = false;
    pt.uminho.di.raft.entities.states.State nextState = null;

    public TimeoutTask(Server srv, long time) {
        this(srv);
        timeout_period = time;
    }

    public TimeoutTask(Server srv) {
        super(srv);
    }

    public pt.uminho.di.raft.entities.states.State getNextState()
    {
        return nextState;
    }

    public boolean isTerminated() {
        return terminated;
    }

    public void setTerminated(boolean term) {
        this.terminated = term;
    }

    public long getTimeoutPeriod() {
        return timeout_period;
    }

    public void setTimeoutPeriod(long timeout) {
        this.timeout_period = timeout;
    }

    public synchronized void setNextState(pt.uminho.di.raft.entities.states.State nextState)
    {
        logger.info(getServer().getIdString() + "TimeoutTask set next state " + nextState + " at " + System.currentTimeMillis());
        this.nextState = nextState;
    }

    @Override
    public void run() {
        logger.info(getServer().getIdString() + "TimeoutTask running...");

        while (isRunning()) {
            synchronized (this) {
                contacted = false;
                
                if(nextState != null)
                {
                    logger.info(getServer().getIdString() + "TimeoutTask for " + getServer().getCurrentRole() + " cycle started. Changing to new state " + nextState);
                    getServer().startRole(nextState);
                    // restart next state vars
                    nextState = null;
                    timeout = false;
                }
                else
                    logger.debug(getServer().getIdString() + "TimeoutTask for " + getServer().getCurrentRole() + " cycle started.");

                long init = -1;
                try {
                    logger.debug(getServer().getIdString() + "TimeoutTask for " + getServer().getCurrentRole() + " waiting for " + timeout_period + "ms...");
                    init = System.currentTimeMillis();
                    wait(timeout_period);
                    long now = System.currentTimeMillis();
                    long waitTime = now - init;
                    logger.debug(getServer().getIdString() + "TimeoutTask for " + getServer().getCurrentRole() + " woke up after " + waitTime + "ms at " + now);

                    if(!contacted) {
                        logger.debug(getServer().getIdString() + "TimeoutTask for " + getServer().getCurrentRole() + " invoking timeout on current server task at " + System.currentTimeMillis());
                        getServer().timeout();
                    }
                } catch (InterruptedException ex) {
                    logger.error(ex.getMessage(), ex);
                    long waitTime = System.currentTimeMillis() - init;
                    logger.info(getServer().getIdString() + "TimeoutTask for " + getServer().getCurrentRole() + " was interrupted after " + waitTime + "ms");
                }
                logger.debug(getServer().getIdString() + "TimeoutTask for " + getServer().getCurrentRole() + " cycle finished.");
            }
        }

        logger.info(getServer().getIdString() + "TimeoutTask for " + getServer().getCurrentRole() + " was terminated!");
    }

    public void heartbeat() {
        logger.debug(getServer().getIdString() + " Server was contacted at " + System.currentTimeMillis());
        contacted = true;
        notifyTask();
    }

    public synchronized void notifyTask()
    {
        State threadState = this.getState();
        logger.debug(getServer().getIdString() + " Notifying TimeoutTask... State=" + threadState);
        
//        if((threadState == Thread.State.TIMED_WAITING) || (threadState == Thread.State.WAITING))
//        {
            this.notify(); // wake up from wait
            logger.debug(getServer().getIdString() + " TimeoutTask notified!");
//        }
//        else
//            logger.debug(getServer().getIdString() + " TimeoutTask not notified! State=" + threadState);
        
    }

    @Override
    public synchronized void terminate() {
        if(isRunning())
        {
            super.terminate();
            logger.debug(getServer().getIdString() + " Terminating TimeoutTask...");
            notifyTask();
        }
        logger.debug(getServer().getIdString() + " Terminated TimeoutTask.");
    }
}
