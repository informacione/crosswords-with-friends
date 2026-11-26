package edu.brown.cs.GROUP.chat;

import edu.brown.cs.GROUP.crosswordswithFriends.GUI;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

/**
 * This class handles the web socket in the situation where there is only one
 * player.
 */
@WebSocket
public class OneChatWebSocketHandler {

  /** This is a hashmap that maps the session id (user id) to the room. */

  private static Map<Session, Integer> userRoom =
      new HashMap<Session, Integer>();

  /** This web socket connects the user to the chatroom.
   *
   * @param user the user
   * @throws Exception in case it break */

  @OnWebSocketConnect
  public void onConnect(Session user) throws Exception {
    synchronized (Chat.getRoomLock()) {
      int nextRoomNumber = GUI.ONEPLAYERID.get() - 1;

      List<Session> usersInRoom = new CopyOnWriteArrayList<Session>();
      usersInRoom.add(user);
      Chat.getRoomUsers().put(nextRoomNumber, usersInRoom);

      userRoom.put(user, nextRoomNumber);
    }
  }

  /** This method releases the user's session when the socket closes.
   *
   * @param user the user
   * @param statusCode the status code
   * @param reason what happened */

  @OnWebSocketClose
  public void onClose(Session user, int statusCode, String reason) {
    synchronized (Chat.getRoomLock()) {
      Integer roomNumber = userRoom.remove(user);
      List<Session> room = Chat.getRoomUsers().get(roomNumber);
      if (room != null && room.remove(user) && room.isEmpty()) {
        Chat.getRoomUsers().remove(roomNumber);
      }
    }
  }

  /** This method returns the room number given a user id.
   *
   * @param user the user id
   * @return the room id */

  public static Integer getRoomNumber(Session user) {
    return userRoom.get(user);
  }

  /** This function determines what message to send to the chatroom depending on
   * whether the user requested data, a letter, or an anagram by clicking on the
   * buttons.
   *
   * @param user the user id
   * @param message the message */

  @OnWebSocketMessage
  public void onMessage(Session user, String message) {
    if (message.startsWith("DATA")) {
      Chat.broadcastCorrect(message, userRoom.get(user));
    } else if (message.startsWith("LETTER")) {
      Chat.broadcastLetter(message, userRoom.get(user));
    } else if (message.startsWith("ANAGRAM")) {
      Chat.broadcastAnagram(message, userRoom.get(user));
    } else if (message.startsWith("**ALL**")) {
      Chat.broadcastAll(user, userRoom.get(user));
    }
  }

}
