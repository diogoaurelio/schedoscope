package org.schedoscope.scheduler.actors

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.Settings
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.scheduler.messages._
import org.schedoscope.schema.ddl.HiveQl
import test.views.ProductBrand

import scala.util.Random
import scala.concurrent.duration._

class DriverActorSpec extends TestKit(ActorSystem("schedoscope"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

  val view = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

  val settings = Settings()
  val transformationManagerActor = TestProbe()
  val brandViewActor = TestProbe()
  val productViewActor = TestProbe()

  def shuffleTransformations:Seq[String] = {
    val transformations = List("filesystem", "hive",
      "mapreduce", "noop", "seq")
    val q = Random.shuffle(transformations)
      .foldLeft(new collection.mutable.Queue[String]()) {
      (growingQueue, transformation) =>
        growingQueue += (transformation)
    }
    Seq(q.dequeue, q.dequeue, q.dequeue)
  }

  trait DriverActorTest {
    val Seq(t1, t2, t3) = shuffleTransformations
    val driverActor1 = TestActorRef(DriverActor.props(settings,
      t1, transformationManagerActor.ref))
    transformationManagerActor.expectMsgPF() {
        case TransformationStatusResponse(msg, actor, driver,
        driverHandle, driverRunStatus) => {
          msg shouldBe "idle"
          actor shouldBe driverActor1
        }
      }
    transformationManagerActor.expectMsg(PullCommand(t1))

    val driverActor2 = TestActorRef(DriverActor.props(settings,
      t2, transformationManagerActor.ref))
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "idle"
        actor shouldBe driverActor2
      }
    }
    transformationManagerActor.expectMsg(PullCommand(t2))

    val driverActor3 = TestActorRef(DriverActor.props(settings,
      t3, transformationManagerActor.ref))
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "idle"
        actor shouldBe driverActor3
      }
    }
    transformationManagerActor.expectMsg(PullCommand(t3))
  }

  trait HiveActorTest {
    val hivedriverActor = TestActorRef(DriverActor.props(settings,
      "hive", transformationManagerActor.ref))
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "idle"
        actor shouldBe hivedriverActor
      }
    }
    transformationManagerActor.expectMsg(PullCommand("hive"))
  }

  "Drivers" should "send pull on preStart and on msg TransformationArrived" in
    new DriverActorTest {
      transformationManagerActor.send(driverActor1, TransformationArrived)
      transformationManagerActor.expectMsg(PullCommand(t1))
      transformationManagerActor.send(driverActor2, TransformationArrived)
      transformationManagerActor.expectMsg(PullCommand(t2))
      transformationManagerActor.send(driverActor3, TransformationArrived)
      transformationManagerActor.expectMsg(PullCommand(t3))
  }

  "Any Driver" should "run DeployCommand" in
    new DriverActorTest {
      val cmd = DriverCommand(DeployCommand(), transformationManagerActor.ref)
      transformationManagerActor.send(driverActor1, cmd)
      transformationManagerActor.expectMsgPF() {
        case TransformationStatusResponse(msg, actor, driver,
        driverHandle, driverRunStatus) => {
          msg shouldBe "deploy"
          actor shouldBe driverActor1
        }
      }
      transformationManagerActor.expectMsg(DeployCommandSuccess())
      transformationManagerActor.expectMsgPF() {
        case TransformationStatusResponse(msg, actor, driver,
        driverHandle, driverRunStatus) => {
          msg shouldBe "idle"
          actor shouldBe driverActor1
        }
      }
      transformationManagerActor.expectMsg(PullCommand(t1))

      transformationManagerActor.send(driverActor2, cmd)
      transformationManagerActor.expectMsgPF() {
        case TransformationStatusResponse(msg, actor, driver,
        driverHandle, driverRunStatus) => {
          msg shouldBe "deploy"
          actor shouldBe driverActor2
        }
      }
      transformationManagerActor.expectMsg(DeployCommandSuccess())
      transformationManagerActor.expectMsgPF() {
        case TransformationStatusResponse(msg, actor, driver,
        driverHandle, driverRunStatus) => {
          msg shouldBe "idle"
          actor shouldBe driverActor2
        }
      }
      transformationManagerActor.expectMsg(PullCommand(t2))

      transformationManagerActor.send(driverActor3, cmd)
      transformationManagerActor.expectMsgPF() {
        case TransformationStatusResponse(msg, actor, driver,
        driverHandle, driverRunStatus) => {
          msg shouldBe "deploy"
          actor shouldBe driverActor3
        }
      }
      transformationManagerActor.expectMsg(DeployCommandSuccess())
      transformationManagerActor.expectMsgPF() {
        case TransformationStatusResponse(msg, actor, driver,
        driverHandle, driverRunStatus) => {
          msg shouldBe "idle"
          actor shouldBe driverActor3
        }
      }
      transformationManagerActor.expectMsg(PullCommand(t3))

    }

  "HiveDriver" should "change state to run hive transformation, " +
    "kill it, and back into idle state" in new HiveActorTest {
    val hiveTransformation = new HiveTransformation(HiveQl.ddl(view))
    val cmd = DriverCommand(TransformView(hiveTransformation, view),
      transformationManagerActor.ref)
    transformationManagerActor.send(hivedriverActor, cmd)
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "running"
        actor shouldBe hivedriverActor
      }
    }
    // A command wrongly sent from transformation manager should be re-enqueued;
    val cmdThoughBusy = DriverCommand(DeployCommand(), transformationManagerActor.ref)
    transformationManagerActor.send(hivedriverActor, cmdThoughBusy)
    transformationManagerActor.expectMsg(cmdThoughBusy)

    transformationManagerActor.send(hivedriverActor, TransformationArrived)
    // should do nothing!
    transformationManagerActor.expectNoMsg(3 seconds)
    // pseudo kill op
    transformationManagerActor.send(hivedriverActor, KillCommand())

    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "idle"
        actor shouldBe hivedriverActor
      }
    }
    transformationManagerActor.expectMsg(PullCommand("hive"))
  }



}
