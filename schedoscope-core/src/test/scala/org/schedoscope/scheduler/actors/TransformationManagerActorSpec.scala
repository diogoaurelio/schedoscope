package org.schedoscope.scheduler.actors
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.Settings
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.transformations.{FilesystemTransformation, Touch}
import org.schedoscope.scheduler.driver.HiveDriver
import org.schedoscope.scheduler.messages._
import test.views.ProductBrand

import scala.concurrent.duration._

class TransformationManagerActorSpec extends TestKit(ActorSystem("schedoscope"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar {

  // common vars
  val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
  lazy val settings = Settings()
  val myTestActor = TestProbe()

  class ForwardChildActor(to: ActorRef) extends Actor {

    def receive = {
      case x => to.forward(x)
    }
  }

  trait TransformationManagerActorTest {

    val hiveDriverRouter = TestProbe()
    val mapRedDriverRouter = TestProbe()
    val noopDriverRouter = TestProbe()
    val seqDriverRouter = TestProbe()
    val fsDriverRouter = TestProbe()

    val transformationManagerActor = TestActorRef(new TransformationManagerActor(settings,
      bootstrapDriverActors = false) {
      override def preStart {
        context.actorOf(Props(new ForwardChildActor(hiveDriverRouter.ref)), "hive-router")
        context.actorOf(Props(new ForwardChildActor(mapRedDriverRouter.ref)), "mapreduce-router")
        context.actorOf(Props(new ForwardChildActor(noopDriverRouter.ref)), "noop-router")
        context.actorOf(Props(new ForwardChildActor(seqDriverRouter.ref)), "seq-router")
        context.actorOf(Props(new ForwardChildActor(fsDriverRouter.ref)), "filesystem-router")
      }
    })

    val idleHiveStatus = TransformationStatusResponse("idle", hiveDriverRouter.ref, null, null, null)
    hiveDriverRouter.send(transformationManagerActor, idleHiveStatus)
    val idleMapRedStatus = TransformationStatusResponse("idle", mapRedDriverRouter.ref, null, null, null)
    mapRedDriverRouter.send(transformationManagerActor, idleMapRedStatus)
    val idleNoopStatus = TransformationStatusResponse("idle", noopDriverRouter.ref, null, null, null)
    noopDriverRouter.send(transformationManagerActor, idleNoopStatus)
    val idleSeqStatus = TransformationStatusResponse("idle", seqDriverRouter.ref, null, null, null)
    seqDriverRouter.send(transformationManagerActor, idleSeqStatus)
    val idleFSStatus = TransformationStatusResponse("idle", fsDriverRouter.ref, null, null, null)
    fsDriverRouter.send(transformationManagerActor, idleFSStatus)

  }

  it should "forward transformations to the correct DriverManager based on incoming View" in
    new TransformationManagerActorTest {
      val msgSender = TestProbe()
      val cmd = DriverCommand(TransformView(testView.transformation(), testView),
        msgSender.ref)
      msgSender.send(transformationManagerActor,testView)
      hiveDriverRouter.expectMsg(cmd)
    }

  it should "forward transformations to the correct DriverManager based on incoming Transformation" in
    new TransformationManagerActorTest {
      val msgSender = TestProbe()
      val filesystemTransformation = new FilesystemTransformation
      val cmd = DriverCommand(filesystemTransformation,
        msgSender.ref)
      //val command = DriverCommand(cmd, self)
      msgSender.send(transformationManagerActor, filesystemTransformation)
      fsDriverRouter.expectMsg(cmd)
      hiveDriverRouter.expectNoMsg(3 seconds)
    }

  it should "multicast deployCommand to routers" in
    new TransformationManagerActorTest {
      val msgSender = TestProbe()
      val cmd = DriverCommand(DeployCommand(), msgSender.ref)
      //val command = DriverCommand(cmd, self)
      msgSender.send(transformationManagerActor, DeployCommand())
      hiveDriverRouter.expectMsg(cmd)
      hiveDriverRouter.reply(DeployCommandSuccess())

      mapRedDriverRouter.expectMsg(cmd)
      mapRedDriverRouter.reply(DeployCommandSuccess())
      msgSender.expectMsg(DeployCommandSuccess())

      noopDriverRouter.expectMsg(cmd)
      noopDriverRouter.reply(DeployCommandSuccess())
      msgSender.expectMsg(DeployCommandSuccess())

      seqDriverRouter.expectMsg(cmd)
      seqDriverRouter.reply(DeployCommandSuccess())
      msgSender.expectMsg(DeployCommandSuccess())

      fsDriverRouter.expectMsg(cmd)
      fsDriverRouter.reply(DeployCommandSuccess())
      msgSender.expectMsg(DeployCommandSuccess())

    }

  it should "return the status of transformations (no running transformations)" in
    new TransformationManagerActorTest {
      val msgSender = TestProbe()
      msgSender.send(transformationManagerActor, GetTransformations())

      msgSender.expectMsgPF() {
        case TransformationStatusListResponse(statusList) => {
          statusList.size shouldBe 5
          statusList should contain(idleHiveStatus)
          statusList should contain(idleFSStatus)
          statusList should contain(idleSeqStatus)
          statusList should contain(idleNoopStatus)
          statusList should contain(idleMapRedStatus)
        }
      }
    }

  it should "return the status of transformations" in
    new TransformationManagerActorTest {
      val msgSender = TestProbe()
      val command = DriverCommand(TransformView(testView.transformation(), testView),
        msgSender.ref)

      msgSender.send(transformationManagerActor, testView)
      hiveDriverRouter.expectMsg(command)

      val busyHiveStatus = TransformationStatusResponse("running", hiveDriverRouter.ref,
        HiveDriver(settings.getDriverSettings("hive")), null, null)
      hiveDriverRouter.send(transformationManagerActor, busyHiveStatus)

      msgSender.send(transformationManagerActor, GetTransformations())

      msgSender.expectMsgPF() {
        case TransformationStatusListResponse(statusList) => {
          statusList.size shouldBe 5
          statusList should contain(busyHiveStatus)
          statusList should contain(idleFSStatus)
          statusList should contain(idleSeqStatus)
          statusList should contain(idleNoopStatus)
          statusList should contain(idleMapRedStatus)
        }
      }
    }

  // integration test transformationManager + DriverRouter + Drivers
  it should "should forward to driver commands without changing the recepient" in {
    val msgSender = TestProbe()
    val transformationManagerActor = TestActorRef(new TransformationManagerActor(settings,
      bootstrapDriverActors = true))
    val cmd = DriverCommand(DeployCommand(), msgSender.ref)
    msgSender.send(transformationManagerActor, cmd)
    msgSender.expectMsg(DeployCommandSuccess())
  }

}
