package org.schedoscope.scheduler.actors
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.Settings
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.transformations.Touch
import org.schedoscope.scheduler.driver.HiveDriver
import org.schedoscope.scheduler.messages._
import test.views.ProductBrand
import scala.concurrent.duration._

class TransformationManagerActorSpec extends TestKit(ActorSystem("schedoscope"))
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar {


  class ForwardChildActor(to: ActorRef) extends Actor {

    def receive = {
      case x => to.forward(x)
    }
  }

  trait TransformationManagerActorTest {
    lazy val settings = Settings()

    val hiveFakeDriver = TestProbe()
    val mapRedFakeDriver = TestProbe()
    val noopFakeDriver = TestProbe()
    val seqFakeDriver = TestProbe()
    val fsFakeDriver = TestProbe()

    val transformationManagerActor = TestActorRef(new TransformationManagerActor(settings,
      bootstrapDriverActors = false) {
        override def preStart {
          context.actorOf(Props(new ForwardChildActor(hiveFakeDriver.ref)), "hive")
          context.actorOf(Props(new ForwardChildActor(mapRedFakeDriver.ref)), "mapreduce")
          context.actorOf(Props(new ForwardChildActor(noopFakeDriver.ref)), "noop")
          context.actorOf(Props(new ForwardChildActor(seqFakeDriver.ref)), "seq")
          context.actorOf(Props(new ForwardChildActor(fsFakeDriver.ref)), "filesystem-0")

        }
    })
    val idleHiveStatus = TransformationStatusResponse("idle", hiveFakeDriver.ref, null, null, null)
    hiveFakeDriver.send(transformationManagerActor, idleHiveStatus)
    val idleMapRedStatus = TransformationStatusResponse("idle", mapRedFakeDriver.ref, null, null, null)
    mapRedFakeDriver.send(transformationManagerActor, idleMapRedStatus)
    val idleNoopStatus = TransformationStatusResponse("idle", noopFakeDriver.ref, null, null, null)
    noopFakeDriver.send(transformationManagerActor, idleNoopStatus)
    val idleSeqStatus = TransformationStatusResponse("idle", seqFakeDriver.ref, null, null, null)
    seqFakeDriver.send(transformationManagerActor, idleSeqStatus)
    val idleFSStatus = TransformationStatusResponse("idle", fsFakeDriver.ref, null, null, null)
    fsFakeDriver.send(transformationManagerActor, idleFSStatus)

    val baseTransformationStatusList = TransformationStatusListResponse(
      List(idleHiveStatus, idleNoopStatus, idleMapRedStatus, idleFSStatus, idleSeqStatus))

  }

  "the TransformationManagerActor" should "enqueue a transformation" in new TransformationManagerActorTest {
    val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))

    transformationManagerActor ! testView
    hiveFakeDriver.expectMsg(TransformationArrived)

    transformationManagerActor ! GetQueues()
    expectMsg(QueueStatusListResponse(Map("filesystem-0" -> List(),
      "mapreduce" -> List(),
      "noop" -> List(),
      "hive" -> List(TransformView(testView.transformation(), testView)),
      "seq" -> List())))
  }

  it should "enqueue a deploy command" in new TransformationManagerActorTest {
    transformationManagerActor ! DeployCommand()

    hiveFakeDriver.expectMsg(TransformationArrived)
    mapRedFakeDriver.expectMsg(TransformationArrived)
    noopFakeDriver.expectMsg(TransformationArrived)
    seqFakeDriver.expectMsg(TransformationArrived)
    fsFakeDriver.expectMsg(TransformationArrived)

    transformationManagerActor ! GetQueues()
    expectMsg(QueueStatusListResponse(Map("filesystem-0" -> List(DeployCommand()),
      "mapreduce" -> List(DeployCommand()),
      "noop" -> List(DeployCommand()),
      "hive" -> List(DeployCommand()),
      "seq" -> List(DeployCommand()))))
  }

  it should "enqueue a filesystem transformation" in new TransformationManagerActorTest {
    transformationManagerActor ! Touch("test")
    fsFakeDriver.expectMsg(TransformationArrived)

    transformationManagerActor ! GetQueues()
    expectMsg(QueueStatusListResponse(Map("filesystem-0" -> List(Touch("test")),
      "mapreduce" -> List(),
      "noop" -> List(),
      "hive" -> List(),
      "seq" -> List())))
  }

  it should "dequeue a transformation if Driver is idle" in new TransformationManagerActorTest {
    val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
    transformationManagerActor ! testView

    hiveFakeDriver.expectMsg(TransformationArrived)
    val command = DriverCommand(TransformView(testView.transformation(), testView), self)

    hiveFakeDriver.send(transformationManagerActor, PullCommand("hive"))
    hiveFakeDriver.expectMsg(command)
    // mark Driver as busy
    hiveFakeDriver.send(transformationManagerActor,
      TransformationStatusResponse("running", hiveFakeDriver.ref,
        HiveDriver(settings.getDriverSettings("hive")), null, null))

    // Should already have been dequeued:
    transformationManagerActor ! GetQueues()
    expectMsg(QueueStatusListResponse(Map("filesystem-0" -> List(),
      "mapreduce" -> List(),
      "noop" -> List(),
      "hive" -> List(),
      "seq" -> List())))

    // Let's try again, and make sure busy Driver does not receive command even after it requested it
    // Should NOT send command to busy actor
    transformationManagerActor ! testView
    hiveFakeDriver.expectMsg(TransformationArrived)
    hiveFakeDriver.send(transformationManagerActor, PullCommand("hive"))
    expectNoMsg(3 seconds)
    transformationManagerActor ! GetQueues()
    // confirm it is sitting there ...
    expectMsg(QueueStatusListResponse(Map("filesystem-0" -> List(),
      "mapreduce" -> List(),
      "noop" -> List(),
      "hive" -> List(TransformView(testView.transformation(), testView)),
      "seq" -> List())))

    hiveFakeDriver.send(transformationManagerActor,
      TransformationStatusResponse("idle", hiveFakeDriver.ref, null, null, null))

    hiveFakeDriver.send(transformationManagerActor, PullCommand("hive"))
    hiveFakeDriver.expectMsg(command)
  }

  it should "dequeue a deploy command" in new TransformationManagerActorTest {
    transformationManagerActor ! DeployCommand()

    hiveFakeDriver.expectMsg(TransformationArrived)
    mapRedFakeDriver.expectMsg(TransformationArrived)
    noopFakeDriver.expectMsg(TransformationArrived)
    seqFakeDriver.expectMsg(TransformationArrived)
    fsFakeDriver.expectMsg(TransformationArrived)

    val command = DriverCommand(DeployCommand(), self)
    // All Drivers should have a deploy command in their Queue
    hiveFakeDriver.send(transformationManagerActor, PullCommand("hive"))
    hiveFakeDriver.expectMsg(command)

    mapRedFakeDriver.send(transformationManagerActor, PullCommand("mapreduce"))
    mapRedFakeDriver.expectMsg(command)

    noopFakeDriver.send(transformationManagerActor, PullCommand("noop"))
    noopFakeDriver.expectMsg(command)

    seqFakeDriver.send(transformationManagerActor, PullCommand("seq"))
    seqFakeDriver.expectMsg(command)

    fsFakeDriver.send(transformationManagerActor, PullCommand("filesystem-0"))
    fsFakeDriver.expectMsg(command)

  }


  it should "dequeue a filesystem transformation" in new TransformationManagerActorTest {
    transformationManagerActor ! Touch("test")
    fsFakeDriver.expectMsg(TransformationArrived)

    val command = DriverCommand(Touch("test"), self)
    fsFakeDriver.send(transformationManagerActor, PullCommand("filesystem-0"))
    fsFakeDriver.expectMsg(command)
  }

  it should "return the status of transformations (no running transformations)" in
    new TransformationManagerActorTest {
      transformationManagerActor ! GetTransformations()
      expectMsg(baseTransformationStatusList)
    }

  it should "return the status of transformations" in
    new TransformationManagerActorTest {
      val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
      val command = DriverCommand(TransformView(testView.transformation(), testView), self)
      transformationManagerActor ! testView
      hiveFakeDriver.expectMsg(TransformationArrived)
      hiveFakeDriver.send(transformationManagerActor, PullCommand("hive"))
      hiveFakeDriver.expectMsg(command)

      val busyHiveStatus = TransformationStatusResponse("running", hiveFakeDriver.ref,
        HiveDriver(settings.getDriverSettings("hive")), null, null)
      hiveFakeDriver.send(transformationManagerActor, busyHiveStatus)

      transformationManagerActor ! GetTransformations()

      val newTransformationStatusList = TransformationStatusListResponse(
        List(idleSeqStatus, busyHiveStatus, idleNoopStatus, idleFSStatus, idleMapRedStatus))
      expectMsg(newTransformationStatusList)

    }


}
