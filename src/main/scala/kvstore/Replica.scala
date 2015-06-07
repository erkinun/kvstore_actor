package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  var persistActor = context.actorOf(persistenceProps)
  context.system.scheduler.schedule(100.milliseconds, 100.milliseconds, self, Timeout)

  override val supervisorStrategy = OneForOneStrategy() {
    case _ : PersistenceException => Restart
  }

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var expected: Long = 0
  var persistMsgs = Map.empty[Long, (ActorRef, Persist)]
  var lastMsgCouple: (Option[ActorRef], Long) = (None, 0L)

  //im ready pick me up
  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      kv = kv + (key -> value)
      //sender ! OperationAck(id) // will need to handle all stuff
      sendPersistMsg(sender, key, Some(value), id)
      context.setReceiveTimeout(1.seconds)
      println("will wait 1 second for insert persistence")
      lastMsgCouple = (Some(sender), id)
    case Remove(key, id) =>
      kv = kv - key
      //sender ! OperationAck(id)
      sendPersistMsg(sender, key, None, id)
      context.setReceiveTimeout(1.seconds)
      println("will wait 1 second for remove persistence")
      lastMsgCouple = (Some(sender), id)
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      val msg = persistMsgs(id)
      msg._1 ! OperationAck(id)
      persistMsgs = persistMsgs - id
    case ReceiveTimeout => //Insert or Remove is not answered
      println("received timeout for a msg")
      lastMsgCouple._1 match {
        case Some(sender) => sender ! OperationFailed(lastMsgCouple._2)
        case None => //do nothing
      }

    case Timeout => //send waiting msgs to persist actor
      persistMsgs foreach (msg => persistActor ! msg._2._2)
    case _ => ???
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case Snapshot(key, valueOption, seq) =>
      if (seq < expected) {
        expected = seq + 1
        sendPersistMsg(sender, key, valueOption, seq)
      }
      else if (seq == expected) {
        valueOption match {
          case Some(str) =>
            kv = kv + (key -> str)
            expected = expected + 1
          case None =>
            kv = kv - key
            expected = expected + 1
        }
        sendPersistMsg(sender, key, valueOption, seq)
      }
      else println("ignoring seq bigger than expected")
    case Persisted(key, id) =>
      val msg = persistMsgs(id)
      msg._1 ! SnapshotAck(key, id)
      persistMsgs = persistMsgs - id

    case Timeout => //send waiting msgs to persist actor
      persistMsgs foreach (msg => persistActor ! msg._2._2)
    case _ =>
  }

  def sendPersistMsg(sender: ActorRef, key: String, valueOption: Option[String], seq: Long): Unit = {
    val msg = Persist(key, valueOption, seq)
    persistMsgs = persistMsgs + (seq -> (sender, msg))
    persistActor ! msg
  }
}

