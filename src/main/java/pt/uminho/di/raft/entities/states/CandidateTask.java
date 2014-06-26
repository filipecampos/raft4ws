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

package pt.uminho.di.raft.entities.states;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;
import org.ws4d.java.CoreFramework;
import org.ws4d.java.types.EndpointReference;
import org.ws4d.java.types.URI;
import pt.uminho.di.raft.entities.Server;
import pt.uminho.di.raft.entities.workers.VoteRequestingTask;

/**
 * *
 *
 * Candidates (§5.2):
 * -	On conversion to candidate, start election:
 *      -Increment currentTerm
 *      -Vote for self
 *      -Reset election timeout
 *      -Send RequestVote RPCs to all other servers
 * -	If votes received from majority of servers: become leader
 * -	If AppendEntries RPC received from new leader: convert to follower
 * -	If election timeout elapses: start new election
 */
public class CandidateTask extends ServerStateTask {

    static Logger logger = Logger.getLogger(CandidateTask.class);

    private ArrayList<EndpointReference> votes;
    
    private ArrayList<VoteRequestingTask> votingTasks;
    private int majorityNum = 0; // 0 for single server

    // mutex
    final Lock lock = new ReentrantLock();
    // condition
    private boolean majorityAchieved = false;
    final Condition majority = lock.newCondition();

    final Semaphore elected = new Semaphore(1);

    public CandidateTask(Server srv)
    {
        super(srv);
    }

    private void startElection()
    {
        votes = new ArrayList<EndpointReference>();
        
//        *      -Increment currentTerm
        getServer().increaseTerm();

        logger.debug(getServer().getIdString() + "Candidate starting election for term " + getServer().getCurrentTerm());

//        *      -Vote for self
        int knownDevices = getServer().getClient().getNumberOfDevices();
        setMaxVoters(knownDevices + 1);
        URI raftSvcAddress = getServer().getRaftService().getAddress();
        getServer().setVotedFor(raftSvcAddress);
        addVote(new EndpointReference(raftSvcAddress));
        
//        *      -Reset election timeout
        server.resetElectionTimeout();
        
//        *      -Send RequestVote RPCs to all other servers
        if(!majorityAchieved && (knownDevices > 0))
        {            
            votingTasks = new ArrayList<VoteRequestingTask>(knownDevices);

            logger.debug(getServer().getIdString() + "Candidate requesting votes from " + knownDevices + " devices...");
            getServer().requestVotes(this);
        }
    }

    @Override
    public synchronized void timeout()
    {
        if(isRunning() && !majorityAchieved)
        {
            // •	If election timeout elapses: start new election
            logger.debug(getServer().getIdString() + "Candidate starting new election as previous one timeouted!");
            startElection();
        }
        else
        {
            logger.error(getServer().getIdString() + "Candidate should have gotten majority and shouldn't have received this timeout.");
            heartbeat();
        }

    }

    public synchronized boolean addVote(EndpointReference epr)
    {
        if(!majorityAchieved && !votes.contains(epr))
        {
            // just process new votes
            votes.add(epr);
            majorityAchieved = votes.size() > majorityNum;

            if(majorityAchieved)
            {
                terminateAllTasks();
                logger.info(getServer().getIdString() + "Candidate has the majority of votes!(" + votes.size() + ">" + majorityNum + ") Turning into leader..." + System.currentTimeMillis());
                if(majorityNum == 0)
                {
                    Thread t = new Thread()
                    {
                        @Override
                        public void run()
                        {
                            try {
                                // wait for 10 ms
                                sleep(10);
                            } catch (InterruptedException ex) {
                                logger.error(ex.getMessage(), ex);
                            }
                            getServer().getTimeoutTask().setNextState(pt.uminho.di.raft.entities.states.State.Leader);
                            getServer().getTimeoutTask().heartbeat();
                        }
                    };
                    CoreFramework.getThreadPool().execute(t);
                }
                else
                {
                    getServer().getTimeoutTask().setNextState(pt.uminho.di.raft.entities.states.State.Leader);
                    getServer().getTimeoutTask().heartbeat();
                }
            }
            logger.debug(getServer().getIdString() + "Candidate added vote for " + epr + "; votes:" + votes.size() + " majority:" + majorityNum);
        }
        else
            logger.debug(getServer().getIdString() + "Vote from " + epr + " wasn't added! majority: " + majorityAchieved);
        
        return majorityAchieved;
    }

    @Override
    public void terminate()
    {
        super.terminate();
        terminateAllTasks();
    }

    @Override
    public void heartbeat()
    {
        logger.debug(getServer().getIdString() + "Candidate turning into a Follower...");
        getServer().getTimeoutTask().setNextState(pt.uminho.di.raft.entities.states.State.Follower);
        getServer().getTimeoutTask().heartbeat();
    }

    @Override
    public void execute()
    {
        startElection();
    }

    public synchronized boolean electedLeader()
    {
        return majorityAchieved;
    }

    public void addTask(VoteRequestingTask voteTask) {
        votingTasks.add(voteTask);
    }

    public void terminateAllTasks()
    {
        if((votingTasks != null) && (!votingTasks.isEmpty()))
        {
            for(VoteRequestingTask aTask : votingTasks)
            {
                aTask.setRunning(false);
                aTask = null;
            }

            votingTasks.clear();
        }
    }

    private void setMaxVoters(int i) {
        majorityNum = i / 2;
    }

}
