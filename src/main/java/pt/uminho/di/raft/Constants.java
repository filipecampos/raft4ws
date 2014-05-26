package pt.uminho.di.raft;

import org.ws4d.java.types.QName;
import org.ws4d.java.types.URI;

/**
 * Copyright 2014 Filipe Campos
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
 *
 */
public abstract class Constants {

    public final static String NameSpace = "http://di.uminho.pt/ws/2014/01/raft";

    public final static String RaftDeviceName = "RAFT_Device";
    public final static QName RaftDeviceQName = new QName(RaftDeviceName, NameSpace);
    // RAFT Service
    public final static String RaftServiceName = "RAFT_Service";
    public final static QName RaftServiceQName = new QName(RaftServiceName, NameSpace);
    public final static URI RaftServiceId = new URI(RaftServiceName);

    public final static String ServerPortName = "ServerPortType";
    public final static QName ServerPortQName = new QName(ServerPortName, NameSpace);

    // RequestVote
    public final static String RequestVoteOperationName = "RequestVote";
    public final static QName RequestVoteOperationQName = new QName(RequestVoteOperationName, NameSpace);

    public final static String RequestVoteRequestTypeName = "RequestVoteRequestType";
    public final static QName RequestVoteRequestTypeQName = new QName(RequestVoteRequestTypeName, NameSpace);

    public final static String RequestVoteRequestElementName = "RequestVoteRequest";
    public final static QName RequestVoteRequestElementQName = new QName(RequestVoteRequestElementName, NameSpace);

    public final static String RequestVoteResponseTypeName = "RequestVoteResponseType";
    public final static QName RequestVoteResponseTypeQName = new QName(RequestVoteResponseTypeName, NameSpace);

    public final static String RequestVoteResponseElementName = "RequestVoteResponse";
    public final static QName RequestVoteResponseElementQName = new QName(RequestVoteResponseElementName, NameSpace);

    // AppendEntries
    public final static String AppendEntriesOperationName = "AppendEntries";
    public final static QName AppendEntriesOperationQName = new QName(AppendEntriesOperationName, NameSpace);

    public final static String AppendEntriesRequestTypeName = "AppendEntriesRequestType";
    public final static QName AppendEntriesRequestTypeQName = new QName(AppendEntriesRequestTypeName, NameSpace);

    public final static String AppendEntriesRequestElementName = "AppendEntriesRequest";
    public final static QName AppendEntriesRequestElementQName = new QName(AppendEntriesRequestElementName, NameSpace);

    public final static String AppendEntriesResponseTypeName = "AppendEntriesResponseType";
    public final static QName AppendEntriesResponseTypeQName = new QName(AppendEntriesResponseTypeName, NameSpace);

    public final static String AppendEntriesResponseElementName = "AppendEntriesResponse";
    public final static QName AppendEntriesResponseElementQName = new QName(AppendEntriesResponseElementName, NameSpace);

    public final static URI emptyURI = new URI("");
    public final static String EntriesTypeName = "EntriesType";
    public final static QName EntriesTypeQName = new QName(EntriesTypeName, NameSpace);

    public final static String EntryTypeName = "EntryType";
    public final static QName EntryTypeQName = new QName(EntryTypeName, NameSpace);

    // InsertCommand
    public final static String InsertCommandOperationName = "InsertCommand";
    public final static QName InsertCommandOperationQName = new QName(InsertCommandOperationName, NameSpace);

    public final static String InsertCommandRequestElementName = "InsertCommandRequest";
    public final static QName InsertCommandRequestElementQName = new QName(InsertCommandRequestElementName, NameSpace);

    public final static String InsertCommandResponseElementName = "InsertCommandResponse";
    public final static QName InsertCommandResponseElementQName = new QName(InsertCommandResponseElementName, NameSpace);

    public final static String InsertCommandResponseTypeName = "InsertCommandResponseType";
    public final static QName InsertCommandResponseTypeQName = new QName(InsertCommandResponseTypeName, NameSpace);

    // Read
    public final static String ReadOperationName = "Read";
    public final static QName ReadOperationQName = new QName(ReadOperationName, NameSpace);

    public final static String ReadRequestElementName = "ReadRequest";
    public final static QName ReadRequestElementQName = new QName(ReadRequestElementName, NameSpace);

    public final static String ReadResponseElementName = "ReadResponse";
    public final static QName ReadResponseElementQName = new QName(ReadResponseElementName, NameSpace);

    public final static String ReadRequestTypeName = "ReadRequestType";
    public final static QName ReadRequestTypeQName = new QName(ReadRequestTypeName, NameSpace);

    public final static String ReadResponseTypeName = "ReadResponseType";
    public final static QName ReadResponseTypeQName = new QName(ReadResponseTypeName, NameSpace);

    // parameters
    public final static String TermElementName = "term";
    public final static QName TermElementQName = new QName(TermElementName, NameSpace);

    public final static String CandidateIdElementName = "candidateId";
    public final static QName CandidateIdElementQName = new QName(CandidateIdElementName, NameSpace);

    public final static String LastLogIndexElementName = "lastLogIndex";
    public final static QName LastLogIndexElementQName = new QName(LastLogIndexElementName, NameSpace);

    public final static String LastLogTermElementName = "lastLogTerm";
    public final static QName LastLogTermElementQName = new QName(LastLogTermElementName, NameSpace);

    public final static String VoteGrantedElementName = "voteGranted";
    public final static QName VoteGrantedElementQName = new QName(VoteGrantedElementName, NameSpace);

    public final static String LeaderIdElementName = "leaderId";
    public final static QName LeaderIdElementQName = new QName("leaderId", NameSpace);

    public final static String PrevLogIndexElementName = "prevLogIndex";
    public final static QName PrevLogIndexElementQName = new QName(PrevLogIndexElementName, NameSpace);

    public final static String PrevLogTermElementName = "prevLogTerm";
    public final static QName PrevLogTermElementQName = new QName(PrevLogTermElementName, NameSpace);

    public final static String IndexElementName = "index";
    public final static QName IndexElementQName = new QName(IndexElementName, NameSpace);

    public final static String CommandElementName = "command";
    public final static QName CommandElementQName = new QName(CommandElementName, NameSpace);

    public final static String ParametersElementName = "parameters";
    public final static QName ParametersElementQName = new QName(ParametersElementName, NameSpace);

    public final static String EntryElementName = "entry";
    public final static QName EntryElementQName = new QName(EntryElementName, NameSpace);

    public final static String EntriesElementName = "entries";
    public final static QName EntriesElementQName = new QName(EntriesElementName, NameSpace);

    public final static String LeaderCommitElementName = "leaderCommit";
    public final static QName LeaderCommitElementQName = new QName(LeaderCommitElementName, NameSpace);

    public final static String SuccessElementName = "success";
    public final static QName SuccessElementQName = new QName(SuccessElementName, NameSpace);

    public final static String CommandTypeName = "CommandType";
    public final static QName CommandTypeQName = new QName(CommandTypeName, NameSpace);

    public final static String UidElementName = "uid";
    public final static QName UidElementQName = new QName(UidElementName, Constants.NameSpace);

    public final static String ResultElementName = "result";
    public final static QName ResultElementQName = new QName(ResultElementName, Constants.NameSpace);

    public final static String LeaderAddressElementName = "leaderAddress";
    public final static QName LeaderAddressElementQName = new QName(LeaderAddressElementName, Constants.NameSpace);
}
