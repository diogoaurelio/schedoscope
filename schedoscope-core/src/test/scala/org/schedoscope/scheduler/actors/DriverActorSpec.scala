package org.schedoscope.scheduler.actors

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.Settings
import org.schedoscope.dsl.Parameter._
import org.schedoscope.scheduler.messages._
import test.views.ProductBrand

import scala.util.Random

class DriverActorSpec extends TestKit(ActorSystem("schedoscope"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

  val view = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
  val brandDependency = view.dependencies.head
  val productDependency = view.dependencies(1)

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

  "Drivers" should "send pull on preStart and on msg TransformationArrived" in
    new DriverActorTest {
      transformationManagerActor.send(driverActor1, TransformationArrived)
      transformationManagerActor.expectMsg(PullCommand(t1))
      transformationManagerActor.send(driverActor2, TransformationArrived)
      transformationManagerActor.expectMsg(PullCommand(t2))
      transformationManagerActor.send(driverActor3, TransformationArrived)
      transformationManagerActor.expectMsg(PullCommand(t3))
  }

  "Drivers" should "change state to running when receiving a command" in
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



}
