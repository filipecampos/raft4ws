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

package pt.uminho.di.raft.entities;

import pt.uminho.di.raft.entities.states.CandidateTask;
import pt.uminho.di.raft.entities.states.State;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.ws4d.java.CoreFramework;
import org.ws4d.java.communication.DPWSCommunicationManager;
import org.ws4d.java.service.DefaultDevice;
import org.ws4d.java.types.EndpointReference;
import org.ws4d.java.types.QNameSet;
import org.ws4d.java.types.URI;
import pt.uminho.di.raft.Constants;
import pt.uminho.di.raft.entities.states.FollowerTask;
import pt.uminho.di.raft.entities.states.LeaderTask;
import pt.uminho.di.raft.entities.states.ServerStateTask;
import pt.uminho.di.raft.entities.workers.TimeoutTask;
import pt.uminho.di.raft.service.RaftService;
import pt.uminho.di.raft.service.log.Log;
import pt.uminho.di.raft.service.log.LogEntry;

/**
 * *
 * Persistent state on all servers:
 * (Updated on stable storage before responding to RPCs)
 *  currentTerm - latest term server has seen (initialized to 0 on first boot, increases monotonically)
 *  votedFor - candidateId that received vote in current term (or null if none)
 *  log[] - log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
 *
 * Volatile state on all servers:
 *  commitIndex - index of highest log entry known to be committed (initialized to 0, increases monotonically)
 *  lastApplied - index of highest log entry applied to state machine (initialized to 0, increases monotonically)
 *
 * Volatile state on leaders:
 * (Reinitialized after election)
 *  nextIndex[] - for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
 *  matchIndex[] - for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
 */
public class Server extends DefaultDevice {

    static Logger logger = Logger.getLogger(Server.class);
    String idString;
    String id;
    int currentTerm;
    URI votedFor;
    Log log;
    private Integer electionTimeout = 1000;
    private Integer minElectionTimeout = 150;
    private Integer maxElectionTimeout = 300;
    HashMap<EndpointReference, Integer> nextIndex;
    HashMap<EndpointReference, Integer> matchIndex;
    State state;
    ServerStateTask currentRole;
    final TimeoutTask timeoutTask;
    ServerClient client;
    RaftService raftService;
    private URI leaderURI;
    private Random random = new Random(System.currentTimeMillis());

    public Server(String id) {
        super(DPWSCommunicationManager.COMMUNICATION_MANAGER_ID);

        this.id = id;
        idString = "[Server " + id + "] - ";
        currentTerm = 0;
        votedFor = null;
        log = new Log(id);
        state = State.Follower;

        timeoutTask = new TimeoutTask(this);

        nextIndex = new HashMap<EndpointReference, Integer>();
        matchIndex = new HashMap<EndpointReference, Integer>();

        this.setPortTypes(new QNameSet(Constants.RaftDeviceQName));
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
        idString = "[Server " + id + "] - ";
    }

    public RaftService getRaftService() {
        return raftService;
    }

    public void setRaftService(RaftService raftService) {
        this.raftService = raftService;
    }

    public int getCommitIndex() {
        return log.getCommitIndex();
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public Integer getElectionTimeout() {
        return electionTimeout;
    }
    
    public Integer getMinElectionTimeout() {
		return minElectionTimeout;
	}

	public void setMinElectionTimeout(Integer minElectionTimeout) {
		this.minElectionTimeout = minElectionTimeout;
	}

	public Integer getMaxElectionTimeout() {
		return maxElectionTimeout;
	}

	public void setMaxElectionTimeout(Integer maxElectionTimeout) {
		this.maxElectionTimeout = maxElectionTimeout;
	}

	public int getLastApplied() {
        return log.getLastApplied();
    }

    public URI getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(URI votedFor) {
        this.votedFor = votedFor;
    }

    public void setState(State state) {
        this.state = state;
    }

    public State getState() {
		return state;
	}

	public Integer getLastLogTerm() {
        return log.getLastLogTerm();
    }

    public Integer getLastLogIndex() {
        return log.getLastLogIndex();
    }

    public LogEntry getLogEntry(Integer index) {
        return log.getEntry(index);
    }

    public LogEntry getLogEntry(String uid) {
        return log.getEntry(uid);
    }

    public String getIdString() {
        return idString;
    }

    public void appendEntries(List<LogEntry> entries) {
        log.append(entries);
    }

    public boolean updateTerm(Integer receivedTerm) {
        boolean updated = false;

        if (receivedTerm > currentTerm) {
            logger.debug(getIdString() + "Server detected higher term=" + receivedTerm + " > currentTerm=" + currentTerm);
            updated = true;
            currentTerm = receivedTerm;
            // If increasing known term for election, votedFor should be null;
            votedFor = null;

            if (state != State.Follower) {
                logger.debug(getIdString() + "Switching to " + State.Follower + " state...");
                timeoutTask.setNextState(State.Follower);
            }
        }

        return updated;
    }

    public void setClient(ServerClient cli) {
        client = cli;
    }

    public ServerClient getClient() {
        return client;
    }

    public void sendHeartbeatOrEntries(boolean checkEntries) {
        client.invokeAppendEntriesOp(checkEntries, currentTerm, leaderURI, log.getCommitIndex());
        logger.debug(idString + " Sent heartbeat or entries at " + System.currentTimeMillis());
    }

    public void updateCommitIndex(Integer leaderCommit) {
        log.updateCommitIndex(leaderCommit);
    }

    public boolean isLeader() {
        return (state == State.Leader);
    }

    public boolean isCandidate() {
        return (state == State.Candidate);
    }

    public boolean isFollower() {
        return (state == State.Follower);
    }

    public LogEntry insertCommand(String uidStr, String command, String parameters) {
        logger.debug(getIdString() + "Server is the Leader! Inserting new command: uid-" + uidStr);

        LogEntry entry = log.getEntry(uidStr);

        if (entry == null) {
            logger.debug(getIdString() + "Inserting New Entry...");
            entry = log.insertNewCommand(uidStr, currentTerm, command, parameters);
            logger.debug(getIdString() + "Entry " + uidStr + " was inserted.");

            int numDevices = 0;
            if (client != null) {
                numDevices = client.getNumberOfDevices();
            }

            logger.debug(getIdString() + "Leader got " + numDevices + " replicas.");
            if (numDevices > 0) {
                entry.setMajorityResponses(numDevices + 1); // replicas + leader
                // leader response
                entry.addLeaderResponse();
                // invoke AppendEntries on replicas to propagate new entry
                timeoutTask.notifyTask();
            } else {
                logger.debug(getIdString() + "Unlocking entry " + uidStr);
                entry.unlock();
            }
        } else {
            logger.debug(getIdString() + "Got existing entry " + uidStr);
        }

        logger.debug(getIdString() + "Finished inserting entry " + uidStr);
        return entry;
    }

    public void setNewLeader(URI newLeaderUri) {
        // leaderURI is the leader's RAFT service address
        votedFor = leaderURI = newLeaderUri;
    }

    public URI getLeader() {
        return leaderURI;
    }

    public void contactedBy(URI leaderId, boolean heartbeat, boolean voteRequest) {
        boolean newLeader = false;

        if (heartbeat) {
            if ((leaderURI == null) || (!leaderURI.equalsWsdRfc3986(leaderId))) {
                logger.debug(getIdString() + "No known leader! Setting " + leaderId + " as new leader.");
                setNewLeader(leaderId);
                newLeader = true;
            }
        } else {
            newLeader = voteRequest;
        }

        if (newLeader || ((leaderId != null) && !leaderId.equalsWsdRfc3986(Constants.emptyURI) && leaderURI.equalsWsdRfc3986(leaderId))) {
            logger.debug(getIdString() + "Received heartbeat from " + leaderId);
            currentRole.heartbeat();
        } else {
            logger.error(getIdString() + "Received invalid heartbeat from " + leaderId + " at " + System.currentTimeMillis());

        }
    }

    public void increaseTerm() {
        currentTerm++;
        votedFor = null;
    }

    public void requestVotes(CandidateTask task) {
        client.invokeRequestVoteOp(task, currentTerm, votedFor, log.getLastLogIndex(), log.getLastLogTerm());
    }

    public void stopServer() throws IOException {
        super.stop();
        timeoutTask.terminate();
    }

    public Integer getNextIndex(EndpointReference dvcEpr) {
        return nextIndex.get(dvcEpr);
    }

    public List<LogEntry> getEntriesFromTo(Integer start, Integer finish) {
        logger.debug(idString + " getEntriesFrom " + start + " to " + finish);

        if (start > finish) {
            start = finish;
        }

        return log.getEntries(start, finish);
    }

    public void startFollowerRole() {
        logger.debug(getIdString() + "Starting Follower role..." + System.currentTimeMillis());
        currentRole = new FollowerTask(this);
        state = State.Follower;
        timeoutTask.setTimeoutPeriod(minElectionTimeout);
        logger.debug(getIdString() + "Finished starting follower role." + System.currentTimeMillis());
    }

    public void startCandidateRole(boolean debug) {
        logger.debug(getIdString() + "Starting Candidate role..." + System.currentTimeMillis());
        currentRole = new CandidateTask(this);
        state = State.Candidate;
        timeoutTask.setTimeoutPeriod(minElectionTimeout);
        if (debug) {
            logger.debug(getIdString() + "Setting Candidate role to not evolve to Leader automatically...");
            currentRole.setRunning(false);
        } else {
            logger.debug(getIdString() + "Running Candidate task..." + System.currentTimeMillis());
            currentRole.execute();
        }

        logger.debug(getIdString() + "Started candidate role." + System.currentTimeMillis());
    }

    public void startLeaderRole() {
        logger.error(getIdString() + "Starting Leader role with " + client.getNumberOfDevices() + " replicas at " + System.currentTimeMillis());
        state = State.Leader;
        leaderURI = votedFor;
        currentRole = new LeaderTask(this);
        timeoutTask.setTimeoutPeriod(minElectionTimeout);

        logger.debug(getIdString() + "Started leader role." + System.currentTimeMillis());
    }

    public boolean commitLogEntry(LogEntry entry) {
        logger.debug(getIdString() + "Server commiting entry " + entry.getUid());
        boolean success = log.commitLogEntry(entry);
        logger.debug(getIdString() + "Server commited entry " + entry.getUid() + " success=" + success);
        if(success)
        {
             if(entry.getTerm() == currentTerm)
             {
                 log.incrementCommitIndex();
             }
        }

        return success;
    }

    public URI getLeaderAddress() {
        return leaderURI;
    }

    public void initializeLeaderVariables() {
        Iterator<EndpointReference> devices = client.getKnownDevices().iterator();
        Integer indexValue = getLastLogIndex() + 1;

        logger.debug(getIdString() + "Initializing nextIndex as " + indexValue + " and matchIndex as 0.");

        while (devices.hasNext()) {
            EndpointReference devRef = devices.next();
            nextIndex.put(devRef, indexValue);
            matchIndex.put(devRef, 0);
        }

    }

    void initializeLeaderVariables(EndpointReference epr) {
        Integer indexValue = getLastLogIndex() + 1;
        nextIndex.put(epr, indexValue);
        matchIndex.put(epr, 0);
    }

    public void receivedReplicaResponse(EndpointReference dvcEpr, Integer lastLogIndex, Integer leaderCommit, List<LogEntry> entries) {
        logger.debug(getIdString() + "Received Replica Response from " + dvcEpr + ". Setting nextIndex=" + (lastLogIndex + 1) + ", matchIndex=" + leaderCommit);
        nextIndex.put(dvcEpr, lastLogIndex + 1);
        matchIndex.put(dvcEpr, leaderCommit);

        if ((entries != null) && (!entries.isEmpty())) {
            for (LogEntry entry : entries) {
                entry.addReplicaResponse();
            }
        }
    }

    public void decrementNextIndex(EndpointReference dvcEpr) {
        Integer currentNextIndex = nextIndex.get(dvcEpr);
        logger.debug(getIdString() + " Decrementing nextIndex for " + dvcEpr + " currently is " + currentNextIndex);
        if (currentNextIndex > 1) {
            nextIndex.put(dvcEpr, currentNextIndex - 1);
        }
    }

    public String getCurrentRole() {
        return state.name();
    }

    public void timeout() {
        if (currentRole != null) {
            currentRole.timeout();
        }
        logger.debug(getIdString() + "Finished processing timeout!");
    }

    public void startRole(State nextState) {
        switch (nextState) {
            case Follower:
                startFollowerRole();
                break;
            case Candidate:
                startCandidateRole(false);
                break;
            case Leader:
                // start remove for normal conditions
                URI raftSvcAddress = getRaftService().getAddress();
                setVotedFor(raftSvcAddress);
                // end remove
                startLeaderRole();
                break;
        }
    }

    public TimeoutTask getTimeoutTask() {
        return timeoutTask;
    }

    public void startTimeoutTask() {
        timeoutTask.setNextState(State.Follower);
        CoreFramework.getThreadPool().execute(timeoutTask);
    }

    public static void main(String[] args) {
        // configure loggers
        PropertyConfigurator.configure("log4j.properties");

        String id = "";
        
        // default Raft values
        Integer min_election_timeout = 150;
        Integer max_election_timeout = 300;

        switch (args.length) {
            case 3:
                min_election_timeout = Integer.parseInt(args[1]);
                max_election_timeout = Integer.parseInt(args[2]);
            case 1:
                id = args[0];
            case 0:
            	logger.error("Usage: ./bin/server <id> <min_timeout(ms)> <max_timeout(ms)>");
            	return;
        }

        try {
            // mandatory: Starting the DPWS Framework.
            CoreFramework.start(args);
            // Create the server device.
            Server device = new Server(id);

            // Create raft service and add it to the device.
            RaftService service = new RaftService(device);
            device.addService(service);

            ServerClient client = new ServerClient(device);
            device.setClient(client);
            device.setMinElectionTimeout(min_election_timeout);
            device.setMaxElectionTimeout(max_election_timeout);
            device.resetElectionTimeout();

            org.ws4d.java.util.Log.setLogLevel(org.ws4d.java.util.Log.DEBUG_LEVEL_ERROR);

            // Starting the device.
            device.start();
            device.startTimeoutTask();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

	public void resetElectionTimeout() {
		electionTimeout = minElectionTimeout + random.nextInt(maxElectionTimeout-minElectionTimeout);
	    timeoutTask.setTimeoutPeriod(electionTimeout);
	}

}
