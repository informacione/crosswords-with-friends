package edu.brown.cs.GROUP.chat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import edu.brown.cs.GROUP.crosswordswithFriends.Crossword;
import edu.brown.cs.GROUP.crosswordswithFriends.GUI;
import edu.brown.cs.GROUP.database.Database;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import spark.Spark;

/**
 * Rooms keep their answer and clue tokens in {@link Chat}'s static censor map.
 * These tests fail while a finished room, an abandoned room, or a room id
 * matchmaking walks away from still holds that set.
 */
public class CensorSetRetentionTest {

  private static final int PORT = 18081;
  private static final int ROOM_BASE = 2000;
  private static final int ONE_PLAYER_BASE = 3000;
  private static final long DEADLINE_MS = 10000L;

  private static WebSocketClient client;
  private final List<Session> openSessions = new ArrayList<Session>();

  /**
   * Client socket that accepts the join and leave messages the server pushes.
   */
  @WebSocket
  public static class SilentClient {
    /**
     * @param message text frame from the server
     */
    @OnWebSocketMessage
    public void onMessage(String message) {
    }

    /**
     * @param error socket failure
     */
    @OnWebSocketError
    public void onError(Throwable error) {
    }
  }

  /**
   * @throws Exception when the database or the server cannot start
   */
  @BeforeClass
  public static void startServer() throws Exception {
    Database database = new Database("data/cluewords.sqlite3");
    new GUI(PORT, database);
    Spark.awaitInitialization();
    client = new WebSocketClient();
    client.start();
  }

  /**
   * @throws Exception when the client or server cannot stop
   */
  @AfterClass
  public static void stopServer() throws Exception {
    if (client != null) {
      client.stop();
    }
    Spark.stop();
  }

  /**
   * Each case starts from an empty room table so retained entries belong to
   * the case itself.
   */
  @Before
  public void resetRooms() {
    discardSessions();
    clearStaticRooms();
  }

  /**
   * Close sockets opened by a case before the next one resets the maps.
   */
  @After
  public void closeSockets() {
    discardSessions();
    awaitNoSessions();
    clearStaticRooms();
  }

  /**
   * Opening the two-player page stores a censor set. A second chat socket
   * advances the room counter, and both disconnects delete the puzzle. The
   * censor set for every finished room must be gone, including while another
   * room is still in play.
   *
   * @throws Exception when a request or socket fails
   */
  @Test
  public void finishedRoomsDropCensorSetsWithoutTouchingTheLiveRoom()
      throws Exception {
    int liveRoom = GUI.TWOPLAYERID.get();
    requestTwoPlayerPage(liveRoom);
    Session liveFirst = connectTwoPlayer();
    Session liveSecond = connectTwoPlayer();
    assertEquals(liveRoom + 1, GUI.TWOPLAYERID.get());
    assertEquals(Integer.valueOf(2), sizeOf(liveRoom));
    assertTrue(Chat.getWordsToCensor().containsKey(liveRoom));

    List<Integer> finished = new ArrayList<Integer>();
    for (int i = 0; i < 3; i++) {
      finished.add(playAndLeave());
    }

    assertEquals(3, new HashSet<Integer>(finished).size());
    for (Integer room : finished) {
      assertNull(GUI.getCrossword(room));
      assertNull(Chat.getRoomUsers().get(room));
      assertFalse("censor set retained for finished room " + room + ": "
          + Chat.getWordsToCensor().get(room),
          Chat.getWordsToCensor().containsKey(room));
    }
    assertTrue("live room " + liveRoom + " must keep its censor set",
        Chat.getWordsToCensor().containsKey(liveRoom));
    assertNotNull(GUI.getCrossword(liveRoom));

    liveFirst.close();
    awaitRoomSize(liveRoom, 1);
    assertTrue(Chat.getWordsToCensor().containsKey(liveRoom));
    liveSecond.close();
    awaitCrosswordGone(liveRoom);
    assertFalse("censor set retained after the last player left room "
        + liveRoom + ": " + Chat.getWordsToCensor().get(liveRoom),
        Chat.getWordsToCensor().containsKey(liveRoom));
    assertTrue(Chat.getWordsToCensor().isEmpty());
  }

  /**
   * Two page loads fill the current room. A one-player page then advances the
   * shared id, and the next two-player page leaves that room id behind. The
   * abandoned id's censor set must not stay in the process.
   *
   * @throws Exception when a request fails
   */
  @Test
  public void matchmakingAbandonDropsOrphanedCensorSet() throws Exception {
    int abandoned = GUI.TWOPLAYERID.get();
    requestTwoPlayerPage(abandoned);
    requestTwoPlayerPage(abandoned);
    assertEquals(2, GUI.getCrossword(abandoned).getPlayers());
    Set<String> orphan = Chat.getWordsToCensor().get(abandoned);
    assertNotNull(orphan);
    assertFalse(orphan.isEmpty());

    int onePlayer = GUI.ONEPLAYERID.get();
    String oneBody = get("/one");
    assertTrue(oneBody.contains(String.valueOf(onePlayer)));
    assertEquals(onePlayer + 1, GUI.ONEPLAYERID.get());
    assertFalse(Chat.getWordsToCensor().containsKey(onePlayer));

    get("/two");
    int replacement = GUI.TWOPLAYERID.get();
    assertNotEquals(abandoned, replacement);
    assertNotNull(Chat.getWordsToCensor().get(replacement));
    assertFalse(Chat.getWordsToCensor().get(replacement).isEmpty());
    assertFalse("censor set orphaned when matchmaking left room " + abandoned
        + ": " + Chat.getWordsToCensor().get(abandoned),
        Chat.getWordsToCensor().containsKey(abandoned));
  }

  /**
   * @return the room id whose puzzle and sockets are already gone
   * @throws Exception when a request or socket fails
   */
  private int playAndLeave() throws Exception {
    int room = GUI.TWOPLAYERID.get();
    requestTwoPlayerPage(room);
    Session first = connectTwoPlayer();
    Session second = connectTwoPlayer();
    assertEquals(room + 1, GUI.TWOPLAYERID.get());
    assertEquals(Integer.valueOf(2), sizeOf(room));
    assertTrue(Chat.getWordsToCensor().containsKey(room));

    first.close();
    awaitRoomSize(room, 1);
    assertNotNull(GUI.getCrossword(room));
    assertTrue(Chat.getWordsToCensor().containsKey(room));

    second.close();
    awaitCrosswordGone(room);
    assertNull(Chat.getRoomUsers().get(room));
    return room;
  }

  /**
   * @param room room id the page is expected to build
   * @throws Exception when the page request fails
   */
  private void requestTwoPlayerPage(int room) throws Exception {
    String body = get("/two");
    assertTrue(body.contains(String.valueOf(room)));
    Crossword puzzle = GUI.getCrossword(room);
    assertNotNull(puzzle);
    assertFalse(puzzle.getFinalList().isEmpty());
    Set<String> words = Chat.getWordsToCensor().get(room);
    assertNotNull(words);
    assertFalse(words.isEmpty());
    assertTrue(words.contains(puzzle.getFinalList().get(0).getWord()));
  }

  /**
   * @param path request path
   * @return response body
   * @throws Exception when the request fails
   */
  private String get(String path) throws Exception {
    HttpURLConnection connection =
        (HttpURLConnection) new URL("http://127.0.0.1:" + PORT + path)
            .openConnection();
    connection.setConnectTimeout(10000);
    connection.setReadTimeout(180000);
    int status = connection.getResponseCode();
    InputStream stream = status >= 400 ? connection.getErrorStream()
        : connection.getInputStream();
    String body = read(stream);
    assertEquals(path + " -> " + status + " " + body, 200, status);
    return body;
  }

  /**
   * @return the connected session
   * @throws Exception when the upgrade fails
   */
  private Session connectTwoPlayer() throws Exception {
    Future<Session> future = client.connect(new SilentClient(),
        new URI("ws://127.0.0.1:" + PORT + "/chat/two"));
    Session session = future.get(15, TimeUnit.SECONDS);
    openSessions.add(session);
    return session;
  }

  /**
   * @param room room id
   * @return number of sockets in the room, or null when the room is gone
   */
  private Integer sizeOf(int room) {
    List<Session> users = Chat.getRoomUsers().get(room);
    if (users == null) {
      return null;
    }
    return Integer.valueOf(users.size());
  }

  /**
   * @param room room id
   * @param size expected socket count
   * @throws Exception when the wait is interrupted
   */
  private void awaitRoomSize(int room, int size) throws Exception {
    long deadline = System.currentTimeMillis() + DEADLINE_MS;
    while (System.currentTimeMillis() < deadline) {
      if (Integer.valueOf(size).equals(sizeOf(room))) {
        return;
      }
      Thread.sleep(20L);
    }
    throw new AssertionError("room " + room + " size stayed " + sizeOf(room));
  }

  /**
   * @param room room id
   * @throws Exception when the wait is interrupted
   */
  private void awaitCrosswordGone(int room) throws Exception {
    long deadline = System.currentTimeMillis() + DEADLINE_MS;
    while (System.currentTimeMillis() < deadline) {
      if (GUI.getCrossword(room) == null) {
        return;
      }
      Thread.sleep(20L);
    }
    throw new AssertionError("crossword for room " + room + " was not removed");
  }

  /**
   * Drop sockets left open by a failed case.
   */
  private void discardSessions() {
    for (Session session : openSessions) {
      try {
        if (session.isOpen()) {
          session.close();
        }
      } catch (Exception ignored) {
        // The next wait observes whether the server finished closing.
      }
    }
    openSessions.clear();
  }

  /**
   * Wait until disconnect handlers have finished removing socket lists.
   */
  private void awaitNoSessions() {
    long deadline = System.currentTimeMillis() + DEADLINE_MS;
    while (System.currentTimeMillis() < deadline) {
      if (Chat.getRoomUsers().isEmpty()) {
        return;
      }
      try {
        Thread.sleep(20L);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /**
   * Clear process-wide room state.
   */
  private static void clearStaticRooms() {
    Chat.getWordsToCensor().clear();
    Chat.getRoomUsers().clear();
    Chat.getUserUsernameMap().clear();
    Chat.getEndGameData().clear();
    GUI.getCrosswordCache().clear();
    GUI.TWOPLAYERID.set(ROOM_BASE);
    GUI.ONEPLAYERID.set(ONE_PLAYER_BASE);
  }

  /**
   * @param stream response bytes
   * @return decoded body
   * @throws Exception when the stream cannot be read
   */
  private static String read(InputStream stream) throws Exception {
    if (stream == null) {
      return "";
    }
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    byte[] chunk = new byte[4096];
    int count;
    while ((count = stream.read(chunk)) != -1) {
      buffer.write(chunk, 0, count);
    }
    stream.close();
    return new String(buffer.toByteArray(), "UTF-8");
  }
}
