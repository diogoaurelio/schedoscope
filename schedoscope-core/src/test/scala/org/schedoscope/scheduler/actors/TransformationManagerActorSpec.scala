package org.schedoscope.scheduler.actors
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.Settings
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.transformations.Touch
import org.schedoscope.scheduler.driver.{HiveDriver}
import org.schedoscope.scheduler.messages._
import test.views.ProductBrand


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

    val hiveDriverActor = TestProbe()
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

  it should "dequeue a transformation" in new TransformationManagerActorTest {
    val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
    transformationManagerActor ! testView
    hiveFakeDriver.expectMsg(TransformationArrived)
    val command = DriverCommand(TransformView(testView.transformation(), testView), self)
    transformationManagerActor ! PullCommand("hive")
    expectMsg(command)
  }

  it should "dequeue a deploy command" in new TransformationManagerActorTest {
    transformationManagerActor ! DeployCommand()

    hiveFakeDriver.expectMsg(TransformationArrived)
    mapRedFakeDriver.expectMsg(TransformationArrived)
    noopFakeDriver.expectMsg(TransformationArrived)
    seqFakeDriver.expectMsg(TransformationArrived)
    fsFakeDriver.expectMsg(TransformationArrived)

    val command = DriverCommand(DeployCommand(), self)
    transformationManagerActor ! PullCommand("hive")
    expectMsg(command)
  }

  it should "dequeue a filesystem transformation" in new TransformationManagerActorTest {
    transformationManagerActor ! Touch("test")
    fsFakeDriver.expectMsg(TransformationArrived)

    val command = DriverCommand(Touch("test"), self)
    transformationManagerActor ! PullCommand("filesystem-0")
    expectMsg(command)
  }

  it should "return the status of transformations (no running transformations)" in
    new TransformationManagerActorTest {
      val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
      transformationManagerActor ! testView
      hiveFakeDriver.expectMsg(TransformationArrived)

      transformationManagerActor ! GetTransformations()
      expectMsg(TransformationStatusListResponse(List()))
    }

  it should "return the status of transformations" in
    new TransformationManagerActorTest {
      val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
      val command = DriverCommand(TransformView(testView.transformation(), testView), self)
      transformationManagerActor ! testView
      hiveFakeDriver.expectMsg(TransformationArrived)

      transformationManagerActor ! PullCommand("hive")
      expectMsg(command)
      transformationManagerActor ! GetTransformations()
      expectMsg(TransformationStatusListResponse(List()))
      val transformationStatusResponse = TransformationStatusResponse("running", hiveDriverActor.ref, HiveDriver(settings.getDriverSettings("hive")), null, null)
      transformationManagerActor ! transformationStatusResponse
      transformationManagerActor ! GetTransformations()
      expectMsg(TransformationStatusListResponse(List(transformationStatusResponse)))
    }


}
