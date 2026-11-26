package edu.brown.cs.GROUP.chat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import edu.brown.cs.GROUP.crosswordswithFriends.Crossword;
import edu.brown.cs.GROUP.crosswordswithFriends.GUI;
import edu.brown.cs.GROUP.database.Database;

import java.net.ServerSocket;
import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import org.eclipse.jetty.websocket.api.Session;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import spark.Spark;

import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.net.http.WebSocket.Listener;

/**
 * Sessions chosen for end-of-round must not stay reachable after the last
 * player disconnects. Chat.broadcastEnd stores the live room list, and
 * TwoChatWebSocketHandler.onClose never removes that entry.
 */
public class EndGameSessionLeakTest {

  private static final int CYCLES = 5;
  private static final long WAIT_SECONDS = 5;
  private static final String SHOW = "**END**:show";
  private static final String CONTINUE = "**END**:continue";

  private static int port;
  private static HttpClient http;
  private static Database database;

  private final List<WebSocket> sockets = new ArrayList<WebSocket>();

  /**
   * Starts the chat socket the same way the game server does.
   * @throws Exception when the server cannot bind
   */
  @BeforeClass
  public static void startChat() throws Exception {
    assertTrue(new java.io.File("cs032_stopwords.txt").isFile());
    try (ServerSocket free = new ServerSocket(0)) {
      port = free.getLocalPort();
    }
    Spark.port(port);
    Chat.initChatroom();
    Spark.awaitInitialization();
    http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(WAIT_SECONDS)).build();
    try {
      database = new Database("data/cluewords.sqlite3");
    } catch (ClassNotFoundException | SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Stops the embedded server.
   */
  @AfterClass
  public static void stopChat() throws SQLException {
    if (http != null) {
      http.close();
    }
    Spark.stop();
    if (database != null) {
      database.close();
    }
  }

  /**
   * Drops chat state left by an earlier case so each case starts empty.
   */
  @Before
  public void clearChatState() {
    Chat.getEndGameData().clear();
    Chat.getRoomUsers().clear();
    Chat.getUserUsernameMap().clear();
    Chat.getWordsToCensor().clear();
    GUI.getCrosswordCache().clear();
  }

  /**
   * Closes any socket a case left open.
   */
  @After
  public void closeSockets() {
    for (WebSocket socket : sockets) {
      try {
        socket.sendClose(WebSocket.NORMAL_CLOSURE, "test")
            .get(WAIT_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException | TimeoutException e) {
        // already closed by the case
      }
    }
    sockets.clear();
  }

  /**
   * One player can store a show choice and disconnect. The closed session
   * must not remain in the end-game map.
   * @throws Exception when the socket fails
   */
  @Test
  public void soleShowDisconnectDoesNotRetainClosedSession() throws Exception {
    int roomId = GUI.TWOPLAYERID.get();
    allowChat(roomId);
    WebSocket socket = connect();
    awaitMembers(roomId, 1);
    Session live = onlySession(roomId);
    send(socket, SHOW);
    awaitChoiceStored(roomId);
    closeSocket(socket);
    awaitRoomGone(roomId);
    assertSessionReleased(live);
  }

  /**
   * Repeating connect, show, and disconnect must not append another entry.
   * @throws Exception when the socket fails
   */
  @Test
  public void repeatedShowDisconnectDoesNotGrowEndGameMap() throws Exception {
    for (int i = 0; i < CYCLES; i++) {
      int roomId = GUI.TWOPLAYERID.get();
      allowChat(roomId);
      WebSocket socket = connect();
      awaitMembers(roomId, 1);
      Session live = onlySession(roomId);
      int before = Chat.getEndGameData().size();
      send(socket, SHOW);
      awaitChoiceStored(roomId);
      closeSocket(socket);
      awaitRoomGone(roomId);
      assertEquals(before, Chat.getEndGameData().size());
      assertSessionReleased(live);
    }
  }

  /**
   * Show still retains the session when a crossword is already in the room.
   * @throws Exception when the socket or puzzle setup fails
   */
  @Test
  public void showWithCrosswordPresentDoesNotRetainClosedSession()
      throws Exception {
    int roomId = GUI.TWOPLAYERID.get();
    Crossword puzzle = new Crossword(database.getAllUnderNine(), database);
    GUI.getCrosswordCache().put(roomId, puzzle);
    Chat.setCensorWords(roomId, puzzle.getFinalList());
    WebSocket socket = connect();
    awaitMembers(roomId, 1);
    Session live = onlySession(roomId);
    send(socket, SHOW);
    awaitChoiceStored(roomId);
    closeSocket(socket);
    awaitRoomGone(roomId);
    assertSessionReleased(live);
  }

  /**
   * Both players choosing show must drop the room entry when both leave.
   * @throws Exception when the socket fails
   */
  @Test
  public void bothShowThenLeaveDoesNotRetainClosedSessions() throws Exception {
    int roomId = GUI.TWOPLAYERID.get();
    allowChat(roomId);
    WebSocket first = connect();
    WebSocket second = connect();
    awaitMembers(roomId, 2);
    List<Session> live = new ArrayList<Session>(Chat.getRoomUsers().get(roomId));
    send(first, SHOW);
    awaitChoiceStored(roomId);
    send(second, SHOW);
    awaitChoiceValue(roomId, 3);
    closeSocket(first);
    closeSocket(second);
    awaitRoomGone(roomId);
    assertSessionsReleased(live);
  }

  /**
   * Show followed by continue uses the same map and must be cleared on leave.
   * @throws Exception when the socket fails
   */
  @Test
  public void showThenContinueDoesNotRetainClosedSessions() throws Exception {
    int roomId = GUI.TWOPLAYERID.get();
    allowChat(roomId);
    WebSocket first = connect();
    WebSocket second = connect();
    awaitMembers(roomId, 2);
    List<Session> live = new ArrayList<Session>(Chat.getRoomUsers().get(roomId));
    send(first, SHOW);
    awaitChoiceStored(roomId);
    send(second, CONTINUE);
    awaitChoiceValue(roomId, 2);
    closeSocket(first);
    closeSocket(second);
    awaitRoomGone(roomId);
    assertSessionsReleased(live);
  }

  /**
   * Continue followed by show stores the room list before the first call
   * returns, and that entry must be gone after both sockets leave.
   * @throws Exception when the socket fails
   */
  @Test
  public void continueThenShowDoesNotRetainClosedSessions() throws Exception {
    int roomId = GUI.TWOPLAYERID.get();
    allowChat(roomId);
    TextListener firstListener = new TextListener();
    TextListener secondListener = new TextListener();
    WebSocket first = connect(firstListener);
    WebSocket second = connect(secondListener);
    awaitMembers(roomId, 2);
    List<Session> live = new ArrayList<Session>(Chat.getRoomUsers().get(roomId));
    send(first, CONTINUE);
    awaitChoiceValue(roomId, 0);
    send(second, SHOW);
    String reply = firstListener.awaitPrefix("**END**:");
    assertTrue(reply.startsWith("**END**:"));
    closeSocket(first);
    closeSocket(second);
    awaitRoomGone(roomId);
    assertSessionsReleased(live);
  }

  private static void allowChat(int roomId) {
    Chat.getWordsToCensor().put(roomId, new HashSet<String>());
  }

  private WebSocket connect() throws Exception {
    return connect(new TextListener());
  }

  private WebSocket connect(TextListener listener) throws Exception {
    WebSocket socket = http.newWebSocketBuilder()
        .connectTimeout(Duration.ofSeconds(WAIT_SECONDS))
        .buildAsync(URI.create("ws://127.0.0.1:" + port + "/chat/two"),
            listener)
        .get(WAIT_SECONDS, TimeUnit.SECONDS);
    sockets.add(socket);
    return socket;
  }

  private static void send(WebSocket socket, String message) throws Exception {
    socket.sendText(message, true).get(WAIT_SECONDS, TimeUnit.SECONDS);
  }

  private void closeSocket(WebSocket socket) throws Exception {
    socket.sendClose(WebSocket.NORMAL_CLOSURE, "leave")
        .get(WAIT_SECONDS, TimeUnit.SECONDS);
    sockets.remove(socket);
  }

  private static void awaitRoomGone(final int roomId)
      throws InterruptedException {
    awaitTrue("room " + roomId + " still registered " + Chat.getRoomUsers(),
        new BooleanSupplier() {
          @Override
          public boolean getAsBoolean() {
            return Chat.getRoomUsers().get(roomId) == null;
          }
        });
  }

  private static Session onlySession(int roomId) {
    List<Session> room = Chat.getRoomUsers().get(roomId);
    assertEquals(1, room.size());
    return room.get(0);
  }

  private static void awaitMembers(final int roomId, final int count)
      throws InterruptedException {
    awaitTrue("room " + roomId + " did not reach " + count + " members",
        new BooleanSupplier() {
          @Override
          public boolean getAsBoolean() {
            List<Session> room = Chat.getRoomUsers().get(roomId);
            return room != null && room.size() == count;
          }
        });
  }

  private static void awaitChoiceStored(final int roomId)
      throws InterruptedException {
    awaitTrue("show or continue did not store room " + roomId,
        new BooleanSupplier() {
          @Override
          public boolean getAsBoolean() {
            List<Session> room = Chat.getRoomUsers().get(roomId);
            return room != null && Chat.getEndGameData().containsKey(room);
          }
        });
  }

  private static void awaitChoiceValue(final int roomId, final int value)
      throws InterruptedException {
    awaitTrue("end-game value for room " + roomId + " was not " + value,
        new BooleanSupplier() {
          @Override
          public boolean getAsBoolean() {
            List<Session> room = Chat.getRoomUsers().get(roomId);
            if (room == null) {
              return false;
            }
            Integer stored = Chat.getEndGameData().get(room);
            return stored != null && stored.intValue() == value;
          }
        });
  }

  private static void assertSessionReleased(Session live) {
    assertFalse(live.isOpen());
    assertFalse("closed session still referenced by the end-game map",
        retained(live));
    assertEquals(0, Chat.getEndGameData().size());
  }

  private static void assertSessionsReleased(List<Session> live) {
    for (Session session : live) {
      assertFalse(session.isOpen());
      assertFalse("closed session still referenced by the end-game map",
          retained(session));
    }
    assertEquals(0, Chat.getEndGameData().size());
  }

  private static boolean retained(Session target) {
    for (Map.Entry<List<Session>, Integer> entry : Chat.getEndGameData()
        .entrySet()) {
      List<Session> room = entry.getKey();
      if (room == null) {
        continue;
      }
      for (Session session : room) {
        if (session == target) {
          return true;
        }
      }
    }
    return false;
  }

  private static void awaitTrue(String message, BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime()
        + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        fail(message + " endGame=" + Chat.getEndGameData());
      }
      Thread.sleep(20);
    }
  }

  /**
   * Records text frames so a continue choice can be observed when it returns.
   */
  private static final class TextListener implements Listener {
    private final List<String> texts = new ArrayList<String>();

    @Override
    public void onOpen(WebSocket webSocket) {
      webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data,
        boolean last) {
      synchronized (texts) {
        texts.add(data.toString());
        texts.notifyAll();
      }
      webSocket.request(1);
      return null;
    }

    /**
     * @param prefix the frame prefix to wait for
     * @return the first text frame with that prefix
     * @throws InterruptedException when the wait is interrupted
     */
    String awaitPrefix(String prefix) throws InterruptedException {
      long deadline = System.nanoTime()
          + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
      synchronized (texts) {
        while (true) {
          for (int i = 0; i < texts.size(); i++) {
            if (texts.get(i).startsWith(prefix)) {
              return texts.remove(i);
            }
          }
          long remaining = deadline - System.nanoTime();
          if (remaining <= 0) {
            fail("no text frame starting with " + prefix + " in " + texts);
          }
          texts.wait(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
        }
      }
    }
  }
}
