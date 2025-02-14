/*******************************************************************************
 * Copyright (c) 2014-2025 Filipe Campos.
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

package pt.uminho.di.raft;

import org.ws4d.java.types.QName;
import org.ws4d.java.types.URI;

public abstract class Constants {

  public static final int NumLogEntries = 15000;
  public static final int DefaultElectionTimeout = 1000;
  public static final int DefaultMinElectionTimeout = 150;
  public static final int DefaultMaxElectionTimeout = 300;

  public static final String NameSpace = "http://di.uminho.pt/ws/2014/01/raft";

  public static final String RaftDeviceName = "RAFT_Device";
  public static final QName RaftDeviceQName = new QName(RaftDeviceName, NameSpace);
  // RAFT Service
  public static final String RaftServiceName = "RAFT_Service";
  public static final QName RaftServiceQName = new QName(RaftServiceName, NameSpace);
  public static final URI RaftServiceId = new URI(RaftServiceName);

  public static final String ServerPortName = "ServerPortType";
  public static final QName ServerPortQName = new QName(ServerPortName, NameSpace);

  // RequestVote
  public static final String RequestVoteOperationName = "RequestVote";
  public static final QName RequestVoteOperationQName =
      new QName(RequestVoteOperationName, NameSpace);

  public static final String RequestVoteRequestTypeName = "RequestVoteRequestType";
  public static final QName RequestVoteRequestTypeQName =
      new QName(RequestVoteRequestTypeName, NameSpace);

  public static final String RequestVoteRequestElementName = "RequestVoteRequest";
  public static final QName RequestVoteRequestElementQName =
      new QName(RequestVoteRequestElementName, NameSpace);

  public static final String RequestVoteResponseTypeName = "RequestVoteResponseType";
  public static final QName RequestVoteResponseTypeQName =
      new QName(RequestVoteResponseTypeName, NameSpace);

  public static final String RequestVoteResponseElementName = "RequestVoteResponse";
  public static final QName RequestVoteResponseElementQName =
      new QName(RequestVoteResponseElementName, NameSpace);

  // AppendEntries
  public static final String AppendEntriesOperationName = "AppendEntries";
  public static final QName AppendEntriesOperationQName =
      new QName(AppendEntriesOperationName, NameSpace);

  public static final String AppendEntriesRequestTypeName = "AppendEntriesRequestType";
  public static final QName AppendEntriesRequestTypeQName =
      new QName(AppendEntriesRequestTypeName, NameSpace);

  public static final String AppendEntriesRequestElementName = "AppendEntriesRequest";
  public static final QName AppendEntriesRequestElementQName =
      new QName(AppendEntriesRequestElementName, NameSpace);

  public static final String AppendEntriesResponseTypeName = "AppendEntriesResponseType";
  public static final QName AppendEntriesResponseTypeQName =
      new QName(AppendEntriesResponseTypeName, NameSpace);

  public static final String AppendEntriesResponseElementName = "AppendEntriesResponse";
  public static final QName AppendEntriesResponseElementQName =
      new QName(AppendEntriesResponseElementName, NameSpace);

  public static final URI emptyURI = new URI("");
  public static final String EntriesTypeName = "EntriesType";
  public static final QName EntriesTypeQName = new QName(EntriesTypeName, NameSpace);

  public static final String EntryTypeName = "EntryType";
  public static final QName EntryTypeQName = new QName(EntryTypeName, NameSpace);

  // InsertCommand
  public static final String InsertCommandOperationName = "InsertCommand";
  public static final QName InsertCommandOperationQName =
      new QName(InsertCommandOperationName, NameSpace);

  public static final String InsertCommandRequestElementName = "InsertCommandRequest";
  public static final QName InsertCommandRequestElementQName =
      new QName(InsertCommandRequestElementName, NameSpace);

  public static final String InsertCommandResponseElementName = "InsertCommandResponse";
  public static final QName InsertCommandResponseElementQName =
      new QName(InsertCommandResponseElementName, NameSpace);

  public static final String InsertCommandResponseTypeName = "InsertCommandResponseType";
  public static final QName InsertCommandResponseTypeQName =
      new QName(InsertCommandResponseTypeName, NameSpace);

  // Read
  public static final String ReadOperationName = "Read";
  public static final QName ReadOperationQName = new QName(ReadOperationName, NameSpace);

  public static final String ReadRequestElementName = "ReadRequest";
  public static final QName ReadRequestElementQName = new QName(ReadRequestElementName, NameSpace);

  public static final String ReadResponseElementName = "ReadResponse";
  public static final QName ReadResponseElementQName =
      new QName(ReadResponseElementName, NameSpace);

  public static final String ReadRequestTypeName = "ReadRequestType";
  public static final QName ReadRequestTypeQName = new QName(ReadRequestTypeName, NameSpace);

  public static final String ReadResponseTypeName = "ReadResponseType";
  public static final QName ReadResponseTypeQName = new QName(ReadResponseTypeName, NameSpace);

  // parameters
  public static final String TermElementName = "term";
  public static final QName TermElementQName = new QName(TermElementName, NameSpace);

  public static final String CandidateIdElementName = "candidateId";
  public static final QName CandidateIdElementQName = new QName(CandidateIdElementName, NameSpace);

  public static final String LastLogIndexElementName = "lastLogIndex";
  public static final QName LastLogIndexElementQName =
      new QName(LastLogIndexElementName, NameSpace);

  public static final String LastLogTermElementName = "lastLogTerm";
  public static final QName LastLogTermElementQName = new QName(LastLogTermElementName, NameSpace);

  public static final String VoteGrantedElementName = "voteGranted";
  public static final QName VoteGrantedElementQName = new QName(VoteGrantedElementName, NameSpace);

  public static final String LeaderIdElementName = "leaderId";
  public static final QName LeaderIdElementQName = new QName("leaderId", NameSpace);

  public static final String PrevLogIndexElementName = "prevLogIndex";
  public static final QName PrevLogIndexElementQName =
      new QName(PrevLogIndexElementName, NameSpace);

  public static final String PrevLogTermElementName = "prevLogTerm";
  public static final QName PrevLogTermElementQName = new QName(PrevLogTermElementName, NameSpace);

  public static final String IndexElementName = "index";
  public static final QName IndexElementQName = new QName(IndexElementName, NameSpace);

  public static final String CommandElementName = "command";
  public static final QName CommandElementQName = new QName(CommandElementName, NameSpace);

  public static final String ParametersElementName = "parameters";
  public static final QName ParametersElementQName = new QName(ParametersElementName, NameSpace);

  public static final String EntryElementName = "entry";
  public static final QName EntryElementQName = new QName(EntryElementName, NameSpace);

  public static final String EntriesElementName = "entries";
  public static final QName EntriesElementQName = new QName(EntriesElementName, NameSpace);

  public static final String LeaderCommitElementName = "leaderCommit";
  public static final QName LeaderCommitElementQName =
      new QName(LeaderCommitElementName, NameSpace);

  public static final String SuccessElementName = "success";
  public static final QName SuccessElementQName = new QName(SuccessElementName, NameSpace);

  public static final String CommandTypeName = "CommandType";
  public static final QName CommandTypeQName = new QName(CommandTypeName, NameSpace);

  public static final String UidElementName = "uid";
  public static final QName UidElementQName = new QName(UidElementName, Constants.NameSpace);

  public static final String ResultElementName = "result";
  public static final QName ResultElementQName = new QName(ResultElementName, Constants.NameSpace);

  public static final String LeaderAddressElementName = "leaderAddress";
  public static final QName LeaderAddressElementQName =
      new QName(LeaderAddressElementName, Constants.NameSpace);
}
