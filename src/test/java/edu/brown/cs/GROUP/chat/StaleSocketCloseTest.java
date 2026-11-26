package edu.brown.cs.GROUP.chat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import edu.brown.cs.GROUP.crosswordswithFriends.Crossword;
import edu.brown.cs.GROUP.crosswordswithFriends.GUI;
import edu.brown.cs.GROUP.crosswordswithFriends.Word;
import edu.brown.cs.GROUP.database.Database;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.jetty.websocket.api.Session;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import spark.Spark;

/**
 * Spec for two-player socket close. The closing session may act on a room
 * only while it is still a member of that room's list. A list that was
 * replaced, or that is replaced while the close is deciding, stays in place
 * and keeps its cached puzzle. These tests fail while close deletes or
 * announces a leave for a session that is no longer in the list.
 *
 * OneChatWebSocketHandler has no close handler and does not remove puzzles.
 */
public class StaleSocketCloseTest {

  private static final int PORT = 18081;
  private static final int ROOM_A = 1000;
  private static final int ROOM_B = 1001;
  private static final Pattern ROOM_ID =
      Pattern.compile("id=(\\d+)\\s+class=\"crossword\"");
  /** Text the two-player page treats as the partner leaving. */
  private static final String DOWN_LEFT = "down left the chat ";
  private static final String ACROSS_LEFT = "across left the chat ";

  private static Database db;
  private static HttpClient http;
  private static final List<WebSocket> OPEN = new ArrayList<WebSocket>();
  private static final Map<WebSocket, CopyOnWriteArrayList<String>> INBOX =
      new IdentityHashMap<WebSocket, CopyOnWriteArrayList<String>>();

  @BeforeClass
  public static void startServer() throws Exception {
    Spark.stop();
    Thread.sleep(400);
    db = new Database("data/cluewords.sqlite3");
    new GUI(PORT, db);
    Spark.awaitInitialization();
    http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10)).build();
  }

  @AfterClass
  public static void stopServer() throws Exception {
    closeOpenSockets();
    Spark.stop();
  }

  @Test(timeout = 180000)
  public void onePlayerPuzzleSurvivesStaleTwoPlayerClose() throws Exception {
    resetIdsAndRooms();
    WebSocket first = connect("/chat/two");
    WebSocket second = connect("/chat/two");
    WebSocket parked = connect("/chat/two");
    waitForRoomSize(ROOM_A, 2);
    waitForRoomSize(ROOM_B, 1);
    assertEquals(ROOM_B, GUI.TWOPLAYERID.get());
    Session parkedSession = onlyMember(ROOM_B);
    assertEquals(Integer.valueOf(ROOM_B), twoPlayerRoom(parkedSession));

    int puzzleId = openOnePlayerPage();
    assertEquals(ROOM_B, puzzleId);
    Crossword puzzle = GUI.getCrossword(puzzleId);
    assertNotNull(puzzle);
    Word word = puzzle.getFinalList().get(0);
    char letter = GUI.getLetter(word.getXIndex(), word.getYIndex(), puzzleId);
    assertFalse("puzzle cell has no letter", letter == '-' || letter == 0);

    WebSocket victim = connect("/chat/one");
    waitUntilAbsent(parkedSession, puzzleId);
    List<Session> victimRoom = members(puzzleId);
    assertEquals(1, victimRoom.size());
    assertFalse(victimRoom.contains(parkedSession));
    assertEquals(Integer.valueOf(puzzleId), twoPlayerRoom(parkedSession));
    Session victimSession = victimRoom.get(0);

    int before = messageCount(victim);
    victim.sendText(letterMessage(word, puzzleId), true).join();
    String hint = waitForNewMessage(victim, before, 5000);
    assertNotNull("letter hint failed before the stale close: "
        + messages(victim), hint);
    assertTrue(hint, hint.startsWith("LETTER;") && hint.endsWith(";" + letter));

    before = messageCount(victim);
    victim.sendText(guessMessage(word, puzzleId), true).join();
    String guess = waitForNewMessage(victim, before, 5000);
    assertNotNull("guess was not accepted before the stale close: "
        + messages(victim), guess);
    assertTrue(guess, guess.startsWith("DATA;"));

    parked.sendClose(WebSocket.NORMAL_CLOSURE, "stale").join();
    assertCloseFinished(parkedSession);

    assertTrue("stale close deleted puzzle " + puzzleId,
        GUI.getCrosswordCache().containsKey(puzzleId));
    assertTrue(GUI.getCrossword(puzzleId) == puzzle);
    List<Session> after = members(puzzleId);
    assertNotNull("stale close dropped the victim's room", after);
    assertTrue("stale close removed the victim", after.contains(victimSession));
    assertFalse(after.contains(parkedSession));

    before = messageCount(victim);
    victim.sendText(letterMessage(word, puzzleId), true).join();
    hint = waitForNewMessage(victim, before, 5000);
    assertNotNull("letter hint stopped resolving after the stale close", hint);
    assertTrue(hint, hint.endsWith(";" + letter));

    before = messageCount(victim);
    victim.sendText(guessMessage(word, puzzleId), true).join();
    guess = waitForNewMessage(victim, before, 5000);
    assertNotNull("guess stopped resolving after the stale close", guess);
    assertTrue(guess, guess.startsWith("DATA;"));

    first.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
    second.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
    victim.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
  }

  @Test(timeout = 180000)
  public void nextOnePlayerGameSurvivesAfterCounterAdvances() throws Exception {
    resetIdsAndRooms();
    connect("/chat/two");
    connect("/chat/two");
    WebSocket parked = connect("/chat/two");
    waitForRoomSize(ROOM_B, 1);
    Session parkedSession = onlyMember(ROOM_B);

    int firstId = openOnePlayerPage();
    assertEquals(ROOM_B, firstId);
    connect("/chat/one");
    waitUntilAbsent(parkedSession, firstId);

    connect("/chat/two");
    waitForRoomSize(firstId, 2);
    assertEquals(firstId + 1, GUI.TWOPLAYERID.get());
    WebSocket nextParked = connect("/chat/two");
    int nextRoom = firstId + 1;
    waitForRoomSize(nextRoom, 1);
    Session nextParkedSession = onlyMember(nextRoom);
    assertEquals(Integer.valueOf(nextRoom), twoPlayerRoom(nextParkedSession));

    int secondId = openOnePlayerPage();
    assertEquals(nextRoom, secondId);
    Crossword second = GUI.getCrossword(secondId);
    WebSocket secondVictim = connect("/chat/one");
    waitUntilAbsent(nextParkedSession, secondId);

    nextParked.sendClose(WebSocket.NORMAL_CLOSURE, "stale-next").join();
    assertCloseFinished(nextParkedSession);
    assertTrue("stale close deleted the next one-player puzzle " + secondId,
        GUI.getCrosswordCache().containsKey(secondId));
    assertTrue(GUI.getCrossword(secondId) == second);
    assertNotNull(members(secondId));
    assertFalse(members(secondId).contains(nextParkedSession));
    secondVictim.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
    parked.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
  }

  @Test(timeout = 180000)
  public void staleCloseDoesNotAnnouncePartnerLeft() throws Exception {
    resetIdsAndRooms();
    connect("/chat/two");
    connect("/chat/two");
    WebSocket firstOccupant = connect("/chat/two");
    waitForRoomSize(ROOM_B, 1);

    HttpResponse<String> two = http.send(HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + PORT + "/two"))
        .timeout(Duration.ofSeconds(60)).GET().build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(two.body(), 200, two.statusCode());
    assertTrue("two-player page did not cache censor words for room " + ROOM_B,
        Chat.getWordsToCensor().containsKey(ROOM_B));

    Session firstSession = onlyMember(ROOM_B);
    firstOccupant.sendClose(WebSocket.NORMAL_CLOSURE, "real-last").join();
    assertCloseFinished(firstSession);
    assertFalse(GUI.getCrosswordCache().containsKey(ROOM_B));
    assertTrue(Chat.getWordsToCensor().containsKey(ROOM_B));

    WebSocket parked = connect("/chat/two");
    waitForRoomSize(ROOM_B, 1);
    Session parkedSession = onlyMember(ROOM_B);
    assertEquals(Integer.valueOf(ROOM_B), twoPlayerRoom(parkedSession));

    int puzzleId = openOnePlayerPage();
    assertEquals(ROOM_B, puzzleId);
    connect("/chat/one");
    waitUntilAbsent(parkedSession, puzzleId);

    WebSocket joiner = connect("/chat/two");
    waitForRoomSize(puzzleId, 2);
    assertFalse(members(puzzleId).contains(parkedSession));
    int mark = messageCount(joiner);

    parked.sendClose(WebSocket.NORMAL_CLOSURE, "stale-broadcast").join();
    assertCloseFinished(parkedSession);
    String trigger = null;
    long deadline = System.currentTimeMillis() + 2000;
    while (System.currentTimeMillis() < deadline && trigger == null) {
      trigger = partnerLeftText(messagesAfter(joiner, mark));
      if (trigger == null) {
        Thread.sleep(20);
      }
    }
    assertNull("stale close sent the partner-left line the two-player page "
        + "turns into single-player mode: " + messagesAfter(joiner, mark),
        trigger);
    assertTrue(GUI.getCrosswordCache().containsKey(puzzleId));
    assertEquals(2, members(puzzleId).size());
    joiner.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
  }

  /**
   * The room list changes inside the membership observation the close handler
   * performs. Re-reading the room after that observation must not delete the
   * puzzle or drop the replacement list.
   */
  @Test(timeout = 120000)
  public void roomReplacedDuringCloseObservationIsKept() throws Exception {
    resetIdsAndRooms();
    connect("/chat/two");
    connect("/chat/two");
    WebSocket parked = connect("/chat/two");
    waitForRoomSize(ROOM_A, 2);
    waitForRoomSize(ROOM_B, 1);
    Session parkedSession = onlyMember(ROOM_B);
    Session stillThere = members(ROOM_A).get(0);
    assertEquals(Integer.valueOf(ROOM_B), twoPlayerRoom(parkedSession));

    Crossword puzzle = new Crossword(db.getAllUnderNine(), db);
    GUI.getCrosswordCache().put(ROOM_B, puzzle);
    Chat.getRoomUsers().put(ROOM_B,
        new ReplacingMemberList(ROOM_B, parkedSession, stillThere));

    parked.sendClose(WebSocket.NORMAL_CLOSURE, "observe").join();
    assertCloseFinished(parkedSession);
    Thread.sleep(200);

    assertTrue("close deleted the puzzle after the room list was replaced",
        GUI.getCrosswordCache().containsKey(ROOM_B));
    assertTrue(GUI.getCrossword(ROOM_B) == puzzle);
    List<Session> kept = members(ROOM_B);
    assertNotNull("close removed the room after the member list was replaced",
        kept);
    assertTrue(kept.contains(stillThere));
    assertFalse(kept.contains(parkedSession));
  }

  @Test(timeout = 120000)
  public void lastMemberCloseRemovesOnlyThatPuzzle() throws Exception {
    resetIdsAndRooms();
    WebSocket alone = connect("/chat/two");
    waitForRoomSize(ROOM_A, 1);
    Session aloneSession = onlyMember(ROOM_A);
    Crossword puzzle = new Crossword(db.getAllUnderNine(), db);
    GUI.getCrosswordCache().put(ROOM_A, puzzle);
    Crossword other = new Crossword(db.getAllUnderNine(), db);
    GUI.getCrosswordCache().put(ROOM_B, other);

    alone.sendClose(WebSocket.NORMAL_CLOSURE, "last").join();
    assertCloseFinished(aloneSession);
    assertFalse(GUI.getCrosswordCache().containsKey(ROOM_A));
    assertNull(members(ROOM_A));
    assertTrue(GUI.getCrossword(ROOM_B) == other);
  }

  @Test(timeout = 120000)
  public void oneOfTwoMembersClosingDoesNotRemovePuzzle() throws Exception {
    resetIdsAndRooms();
    WebSocket first = connect("/chat/two");
    WebSocket second = connect("/chat/two");
    waitForRoomSize(ROOM_A, 2);
    Session closing = members(ROOM_A).get(0);
    Session staying = members(ROOM_A).get(1);
    Crossword puzzle = new Crossword(db.getAllUnderNine(), db);
    GUI.getCrosswordCache().put(ROOM_A, puzzle);

    first.sendClose(WebSocket.NORMAL_CLOSURE, "one-of-two").join();
    assertCloseFinished(closing);
    assertTrue(GUI.getCrossword(ROOM_A) == puzzle);
    List<Session> left = members(ROOM_A);
    assertNotNull(left);
    assertFalse(left.contains(closing));
    assertTrue(left.contains(staying));
    second.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
  }

  /**
   * List installed as a room's membership. The first observation publishes a
   * different list for that room, the same replacement a one-player connect
   * performs if it lands between a check and a later read.
   */
  private static final class ReplacingMemberList extends ArrayList<Session> {
    private static final long serialVersionUID = 1L;
    private final int roomId;
    private final Session replacement;
    private boolean replaced;

    ReplacingMemberList(int roomId, Session member, Session replacement) {
      super();
      add(member);
      this.roomId = roomId;
      this.replacement = replacement;
    }

    private void replaceRoomList() {
      if (replaced) {
        return;
      }
      replaced = true;
      List<Session> victim = new ArrayList<Session>();
      victim.add(replacement);
      Chat.getRoomUsers().put(roomId, victim);
    }

    @Override
    public int size() {
      replaceRoomList();
      return super.size();
    }

    @Override
    public boolean contains(Object o) {
      replaceRoomList();
      return super.contains(o);
    }

    @Override
    public boolean remove(Object o) {
      replaceRoomList();
      return super.remove(o);
    }

    @Override
    public Session get(int index) {
      replaceRoomList();
      return super.get(index);
    }

    @Override
    public Iterator<Session> iterator() {
      replaceRoomList();
      return super.iterator();
    }

    @Override
    public Object[] toArray() {
      replaceRoomList();
      return super.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      replaceRoomList();
      return super.toArray(a);
    }

    @Override
    public boolean isEmpty() {
      replaceRoomList();
      return super.isEmpty();
    }
  }

  private static void resetIdsAndRooms() throws Exception {
    closeOpenSockets();
    Thread.sleep(200);
    Chat.getRoomUsers().clear();
    Chat.getUserUsernameMap().clear();
    Chat.getWordsToCensor().clear();
    Chat.getEndGameData().clear();
    GUI.getCrosswordCache().clear();
    GUI.TWOPLAYERID.set(ROOM_A);
    GUI.ONEPLAYERID.set(ROOM_B);
    clearHandlerRooms();
  }

  private static void closeOpenSockets() {
    for (WebSocket socket : new ArrayList<WebSocket>(OPEN)) {
      try {
        socket.sendClose(WebSocket.NORMAL_CLOSURE, "reset").join();
      } catch (Exception ignored) {
        socket.abort();
      }
    }
    OPEN.clear();
    INBOX.clear();
  }

  @SuppressWarnings("unchecked")
  private static void clearHandlerRooms() throws Exception {
    for (Class<?> type : new Class<?>[] {TwoChatWebSocketHandler.class,
        OneChatWebSocketHandler.class}) {
      Field field = type.getDeclaredField("userRoom");
      field.setAccessible(true);
      ((Map<Session, Integer>) field.get(null)).clear();
    }
  }

  @SuppressWarnings("unchecked")
  private static Integer twoPlayerRoom(Session session) throws Exception {
    Field field = TwoChatWebSocketHandler.class.getDeclaredField("userRoom");
    field.setAccessible(true);
    return ((Map<Session, Integer>) field.get(null)).get(session);
  }

  private static WebSocket connect(String path) {
    CopyOnWriteArrayList<String> inbox = new CopyOnWriteArrayList<String>();
    WebSocket socket = http.newWebSocketBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .buildAsync(URI.create("ws://127.0.0.1:" + PORT + path),
            new WebSocket.Listener() {
              @Override
              public void onOpen(WebSocket webSocket) {
                webSocket.request(Long.MAX_VALUE);
              }

              @Override
              public CompletionStage<?> onText(WebSocket webSocket,
                  CharSequence data, boolean last) {
                inbox.add(data.toString());
                webSocket.request(1);
                return null;
              }
            }).join();
    OPEN.add(socket);
    INBOX.put(socket, inbox);
    return socket;
  }

  private static int openOnePlayerPage() throws Exception {
    HttpResponse<String> response = http.send(HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + PORT + "/one"))
        .timeout(Duration.ofSeconds(60)).GET().build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(response.body(), 200, response.statusCode());
    Matcher matcher = ROOM_ID.matcher(response.body());
    assertTrue("one-player page did not name a room: " + response.body(),
        matcher.find());
    return Integer.parseInt(matcher.group(1));
  }

  private static List<Session> members(int room) {
    return Chat.getRoomUsers().get(room);
  }

  private static Session onlyMember(int room) {
    List<Session> sessions = members(room);
    assertNotNull(sessions);
    assertEquals(1, sessions.size());
    return sessions.get(0);
  }

  private static void waitForRoomSize(int room, int size) throws Exception {
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline) {
      List<Session> sessions = members(room);
      if (sessions != null && sessions.size() == size) {
        return;
      }
      Thread.sleep(20);
    }
    assertEquals("room " + room + " membership " + members(room), size,
        members(room) == null ? 0 : members(room).size());
  }

  private static void waitUntilAbsent(Session session, int room)
      throws Exception {
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline) {
      List<Session> sessions = members(room);
      if (sessions != null && !sessions.contains(session)) {
        return;
      }
      Thread.sleep(20);
    }
    assertFalse(members(room).contains(session));
  }

  private static void assertCloseFinished(Session session) throws Exception {
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline) {
      if (twoPlayerRoom(session) == null
          && !Chat.getUserUsernameMap().containsKey(session)) {
        return;
      }
      Thread.sleep(20);
    }
    assertNull(twoPlayerRoom(session));
    assertFalse(Chat.getUserUsernameMap().containsKey(session));
  }

  private static int messageCount(WebSocket socket) {
    return INBOX.get(socket).size();
  }

  private static List<String> messages(WebSocket socket) {
    return new ArrayList<String>(INBOX.get(socket));
  }

  private static List<String> messagesAfter(WebSocket socket, int mark) {
    List<String> all = messages(socket);
    if (mark >= all.size()) {
      return new ArrayList<String>();
    }
    return new ArrayList<String>(all.subList(mark, all.size()));
  }

  private static String waitForNewMessage(WebSocket socket, int mark, long ms)
      throws Exception {
    long deadline = System.currentTimeMillis() + ms;
    while (System.currentTimeMillis() < deadline) {
      List<String> inbox = INBOX.get(socket);
      if (inbox.size() > mark) {
        return inbox.get(mark);
      }
      Thread.sleep(20);
    }
    return null;
  }

  private static String letterMessage(Word word, int puzzleId) {
    return "LETTER;" + word.getXIndex() + ";" + word.getYIndex() + ";"
        + puzzleId;
  }

  private static String guessMessage(Word word, int puzzleId) {
    return "DATA;" + word.getWord() + ";" + word.getXIndex() + ";"
        + word.getYIndex() + ";" + word.getOrientation() + ";" + puzzleId;
  }

  private static String partnerLeftText(List<String> payloads) {
    for (String payload : payloads) {
      try {
        String html = new org.json.JSONObject(payload).getString("userMessage");
        String[] parts = html.split("<p>");
        if (parts.length < 2) {
          continue;
        }
        String inner = parts[1].split("</p>")[0];
        if (DOWN_LEFT.equals(inner) || ACROSS_LEFT.equals(inner)) {
          return inner;
        }
      } catch (Exception ignored) {
        // Not a chat payload.
      }
    }
    return null;
  }
}
