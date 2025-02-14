package pt.uminho.di.raft.service.log;

import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.ws4d.java.CoreFramework;

public class LogTest {
  static Log log;

  static List<LogEntry> masterEntries =
      List.of(
          new LogEntry("master-uuid1", 1, 1, "x", "1"),
          new LogEntry("master-uuid2", 2, 1, "y", "11"),
          new LogEntry("master-uuid3", 3, 3, "z", "20"),
          new LogEntry("master-uuid4", 4, 3, "w", "33"),
          new LogEntry("master-uuid5", 5, 4, "x", "400"));

  static List<LogEntry> moreEntries =
      List.of(
          new LogEntry("master-uuid11", 6, 4, "x", "500"),
          new LogEntry("master-uuid22", 7, 5, "y", "22"),
          new LogEntry("master-uuid33", 8, 5, "z", "40"),
          new LogEntry("master-uuid44", 9, 6, "w", "66"),
          new LogEntry("master-uuid55", 10, 6, "x", "800"));

  @Before
  public void setUp() {
    CoreFramework.start(null);
    log = new Log("myId");
  }

  @After
  public void tearDown() throws Exception {
    CoreFramework.stopIgnoringInstancesCount();
    log = null;
  }

  @Test
  public void verifyEmptyLogVars() {
    // given no ops on log
    // when nothing is done
    // then
    Assert.assertEquals(Integer.valueOf(-1), log.getLastLogTerm());
    Assert.assertEquals(Integer.valueOf(0), log.getCommitIndex());
    Assert.assertEquals(Integer.valueOf(0), log.getLastLogIndex());
  }

  @Test
  public void insertAndRetrieveLogEntry() {
    // given an empty log
    int term = 1;
    String entryUuid = "follower-uuid1";
    String command = "x";
    String parameters = "1";

    // when
    LogEntry logEntry = log.insertNewCommand(entryUuid, term, command, parameters);
    LogEntry sameLogEntry = log.getEntry(entryUuid);

    // then
    Assert.assertNotNull(logEntry);
    Assert.assertNotNull(sameLogEntry);
    Assert.assertEquals(logEntry, sameLogEntry);
    Assert.assertEquals(Integer.valueOf(term), log.getLastLogTerm());
    Assert.assertEquals(Integer.valueOf(0), log.getCommitIndex());
    Assert.assertEquals(Integer.valueOf(1), log.getLastLogIndex());
    Assert.assertEquals(entryUuid, logEntry.getUid());
    Assert.assertEquals(Integer.valueOf(term), logEntry.getTerm());
    Assert.assertEquals(Integer.valueOf(1), logEntry.getIndex());
    Assert.assertEquals(command, logEntry.getCommand());
    Assert.assertEquals(parameters, logEntry.getParameters());
    Assert.assertNull(logEntry.getSuccess()); // Field set by client

    logEntry.unlock(); // Need to unlock. Otherwise, blocks until majority of responses is received
    Assert.assertEquals("OK", logEntry.getResult());
  }

  @Test
  public void failRetrievingLogEntryFromEmptyLog() {
    // given an empty log

    // when
    LogEntry logEntry = log.getEntry(1);
    LogEntry anotherLogEntry = log.getEntry("follower-uuid1");

    // then
    Assert.assertNull(logEntry);
    Assert.assertNull(anotherLogEntry);
  }

  @Test
  public void failRetrievingInexistentLogEntryFromLog() {
    // given
    log.append(masterEntries);
    log.updateCommitIndex(5);

    // when
    LogEntry logEntry = log.getEntry(6);
    LogEntry anotherLogEntry = log.getEntry("follower-uuid1");

    // then
    Assert.assertNull(logEntry);
    Assert.assertNull(anotherLogEntry);
  }

  @Test
  public void appendMasterEntriesToEmptyLog() {
    // given an empty log

    // when
    log.append(masterEntries);
    log.updateCommitIndex(5);

    // then
    List<LogEntry> logEntries = log.getEntries(1, 5 + 1);
    Assert.assertEquals(masterEntries, logEntries);
    Assert.assertEquals(Integer.valueOf(4), log.getLastLogTerm());
    Assert.assertEquals(Integer.valueOf(5), log.getCommitIndex());
    Assert.assertEquals(Integer.valueOf(5), log.getLastLogIndex());
  }

  @Test
  public void appendMasterEntriesToEmptyLogTwice() {
    // given an empty log
    log.append(masterEntries);
    log.updateCommitIndex(5);

    // when
    log.append(moreEntries);
    log.updateCommitIndex(10);

    // then
    List<LogEntry> expectedEntries = new ArrayList<>(masterEntries.size() + moreEntries.size());
    expectedEntries.addAll(masterEntries);
    expectedEntries.addAll(moreEntries);
    List<LogEntry> logEntries = log.getEntries(1, 10 + 1);
    Assert.assertEquals(expectedEntries, logEntries);
    Assert.assertEquals(Integer.valueOf(6), log.getLastLogTerm());
    Assert.assertEquals(Integer.valueOf(10), log.getCommitIndex());
    Assert.assertEquals(Integer.valueOf(10), log.getLastLogIndex());
  }

  @Test
  public void appendMasterEntriesToConflictingLog() {
    // given
    int term = 1;
    log.insertNewCommand("follower-uuid1", term, "x", "1");
    log.insertNewCommand("follower-uuid2", term, "x", "2");
    log.insertNewCommand("follower-uuid3", ++term, "x", "5");

    Assert.assertEquals(Integer.valueOf(term), log.getLastLogTerm());
    Assert.assertEquals(Integer.valueOf(0), log.getCommitIndex());
    Assert.assertEquals(Integer.valueOf(3), log.getLastLogIndex());

    // when
    log.append(masterEntries);
    log.updateCommitIndex(5);

    // then
    List<LogEntry> logEntries = log.getEntries(1, 5 + 1);

    List<LogEntry> expectedEntries = new ArrayList<>(5);
    expectedEntries.add(log.getEntry(1));
    expectedEntries.add(log.getEntry("follower-uuid2"));
    expectedEntries.add(masterEntries.get(2));
    expectedEntries.add(masterEntries.get(3));
    expectedEntries.add(masterEntries.get(4));
    Assert.assertEquals(expectedEntries, logEntries);
    Assert.assertEquals(Integer.valueOf(4), log.getLastLogTerm());
    Assert.assertEquals(Integer.valueOf(5), log.getCommitIndex());
    Assert.assertEquals(Integer.valueOf(5), log.getLastLogIndex());
  }
}
