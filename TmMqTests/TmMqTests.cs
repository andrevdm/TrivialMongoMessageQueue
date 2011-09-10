using System;
using System.Configuration;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Driver;
using TmMq;

namespace TmMqTests
{
    [TestClass]
    public class TmMqTests
    {
        [TestInitialize]
        public void TestInitialize()
        {
            string conStr = ConfigurationManager.AppSettings[ "MongoDB.Server" ];
            var server = MongoServer.Create( conStr );
            var db = server[ "tmmq" ];

            Action<string> drop = s =>
                                      {
                                          if( db.CollectionExists( s ) )
                                          {
                                              db.DropCollection( s );
                                          }
                                      };

            drop( "error" );
            drop( "TestSendBeforeReceiveStarted" );
            drop( "TestSendAfterReceiveStarted" );
            drop( "TestStartReceive" );
            drop( "TestProperties" );
            drop( "TestRedeliver" );
            drop( "TestErrorQueue" );
        }

        [TestMethod]
        public void TestSendBeforeReceiveStarted()
        {
            using( var send = new TmMqSender( "TestSendBeforeReceiveStarted" ) )
            {
                var msg = new TmMqMessage();
                msg.Text = "msg1";
                send.Send( msg );
            }

            using( var recv = new TmMqReceiver( "TestSendBeforeReceiveStarted" ) )
            {
                var count = recv.CountPending();
                Assert.AreEqual( 1, count, "Should be one pending item" );

                ITmMqMessage recieved = recv.Receive().FirstOrDefault();

                Assert.IsNotNull( recieved );
                Assert.AreEqual( "msg1", recieved.Text );
            }
        }

        [TestMethod]
        public void TestProperties()
        {
            using( var send = new TmMqSender( "TestProperties" ) )
            {
                var msg = new TmMqMessage();
                msg.Text = "msg1";
                msg.Properties.String = "string";
                msg.Properties.Int = 1234;
                msg.Properties.Complex = new Complex{ Prop1 = 1, PropD = 123.456, PropS = "abcdefg" };
                send.Send( msg );
            }

            using( var recv = new TmMqReceiver( "TestProperties" ) )
            {
                var count = recv.CountPending();
                Assert.AreEqual( 1, count, "Should be one pending item" );

                ITmMqMessage recieved = recv.Receive().FirstOrDefault();

                Assert.IsNotNull( recieved );
                Assert.AreEqual( "msg1", recieved.Text );
                Assert.AreEqual( "string", recieved.Properties.String, "Invalid recieved.Properties.String" );
                Assert.AreEqual( 1234, recieved.Properties.Int, "Invalid recieved.Properties.Int" );
                Assert.IsInstanceOfType( recieved.Properties.Complex, typeof(Complex) );
                Assert.AreEqual( 1, recieved.Properties.Complex.Prop1, "Invalid recieved.Properties.Complex.Prop1" );
                Assert.AreEqual( 123.456D, recieved.Properties.Complex.PropD, "Invalid recieved.Properties.Complex.PropD" );
                Assert.AreEqual( "abcdefg", recieved.Properties.Complex.PropS, "Invalid recieved.Properties.Complex.PropS" );
            }
        }

        [TestMethod]
        public void TestSendAfterReceiveStarted()
        {
            using( var recv = new TmMqReceiver( "TestSendAfterReceiveStarted" ) )
            {
                using( var send = new TmMqSender( "TestSendAfterReceiveStarted" ) )
                {
                    var msg = new TmMqMessage();
                    msg.Text = "msg1";
                    send.Send( msg );
                }

                var count = recv.CountPending();
                Assert.AreEqual( 1, count, "Should be one pending item" );

                ITmMqMessage recieved = recv.Receive().FirstOrDefault();

                Assert.IsNotNull( recieved );
                Assert.AreEqual( "msg1", recieved.Text );
            }
        }

        [TestMethod]
        public void TestStartReceive()
        {
            using( var recv = new TmMqReceiver( "TestStartReceive" ) )
            {
                ITmMqMessage recieved = null;

                using( var evt = new ManualResetEvent( false ) )
                {
                    using( var send = new TmMqSender( "TestStartReceive" ) )
                    {
                        recv.StartReceiving( 1, m =>
                                                {
                                                    recieved = m;
                                                    evt.Set();
                                                } );

                        var msg = new TmMqMessage();
                        msg.Text = "msg1";
                        send.Send( msg );
                    }

                    evt.WaitOne( 500 );
                }

                Assert.IsNotNull( recieved );
                Assert.AreEqual( "msg1", recieved.Text );
            }
        }

        [TestMethod]
        public void TestPubSub()
        {
            using( var rcvr1 = new TmMqPubSubReceiver( "TestPubSub" ) )
            using( var rcvr2 = new TmMqPubSubReceiver( "TestPubSub" ) )
            using( var rcvr3 = new TmMqPubSubReceiver( "TestPubSub" ) )
            using( var rcvr4 = new TmMqPubSubReceiver( "TestPubSub" ) )
            {
                var r1 = new List<ITmMqMessage>();
                var r2 = new List<ITmMqMessage>();
                var r3 = new List<ITmMqMessage>();
                var r4 = new List<ITmMqMessage>();

                rcvr1.StartReceiving( 1, r1.Add );
                rcvr2.StartReceiving( 1, r2.Add );
                rcvr3.StartReceiving( 1, r3.Add );
                rcvr4.StartReceiving( 1, r4.Add );

                using( var sender = new TmMqPubSubSender( "TestPubSub" ) )
                {
                    for( int i = 0; i < 10; ++i )
                    {
                        var msg = new TmMqMessage();
                        msg.Text = "ps-" + i;
                        sender.Send( msg );
                    }
                }

                int retry = 40;

                while( (retry-- > 0) && (r1.Count + r2.Count + r3.Count + r4.Count != 40) )
                {
                    Thread.Sleep( 30 );
                }

                Assert.AreEqual( 10, r1.Count, "r1 - Wrong number of messages received" );
                Assert.AreEqual( 10, r2.Count, "r2 - Wrong number of messages received" );
                Assert.AreEqual( 10, r3.Count, "r3 - Wrong number of messages received" );
                Assert.AreEqual( 10, r4.Count, "r4 - Wrong number of messages received" );
            }
        }
        
        [TestMethod]
        public void TestRedeliver()
        {
            using( var send = new TmMqSender( "TestRedeliver" ) )
            {
                var msg = new TmMqMessage();
                msg.Text = "msg1";
                send.Send( msg );
            }

            using( var recv = new TmMqReceiver( "TestRedeliver" ) )
            {
                var count = recv.CountPending();
                Assert.AreEqual( 1, count, "Should be one pending item" );

                ITmMqMessage recieved = null;

                int retry = 0;

                using( var evt = new ManualResetEvent( false ) )
                {
                    recv.StartReceiving( 1, msg =>
                                                {
                                                    if( retry++ == 0 )
                                                    {
                                                        throw new Exception( "fail " + retry );
                                                    }

                                                    recieved = msg;
                                                    evt.Set();
                                                } );
                    evt.WaitOne( 5000 );
                }

                Assert.IsNotNull( recieved );
                Assert.AreEqual( "msg1", recieved.Text );
                Assert.AreEqual( 1, recieved.Errors.Count, "An error should have been logged" );
            }
        }

        [TestMethod]
        public void TestErrorQueue()
        {
            using( var send = new TmMqSender( "TestErrorQueue" ) )
            {
                var msg = new TmMqMessage();
                msg.Text = "msg1 - fail";
                send.Send( msg );
            }

            using( var recv = new TmMqReceiver( "TestErrorQueue" ) )
            {
                int fail = 0;

                using( var evt = new ManualResetEvent( false ) )
                {
                    recv.StartReceiving( 1, msg =>
                                                {
                                                    if( fail++ == 2 )
                                                    {
                                                        evt.Set();
                                                    }

                                                    throw new Exception( "fail " );
                                                } );

                    evt.WaitOne( 5000 );
                }
            }

            using( var recv = new TmMqReceiver( "error" ) )
            {
                ITmMqMessage errorMsg = null;

                using( var evt = new ManualResetEvent( false ) )
                {
                    recv.StartReceiving( 1, msg =>
                    {
                        errorMsg = msg;
                        evt.Set();
                    } );

                    evt.WaitOne( 5000 );
                }

                Assert.IsNotNull( errorMsg );
                Assert.AreEqual( "msg1 - fail", errorMsg.Text );
                Assert.AreEqual( "TestErrorQueue", errorMsg.OriginalQueue );
            }
        }

        public TestContext TestContext { get; set; }
    }

    [Serializable]
    public class Complex
    {
        public int Prop1 { get; set; }
        public double PropD { get; set; }
        public string PropS { get; set; }
    }

    public class TmMqTestConfig : TmMqConfig
    {
        public TmMqTestConfig()
        {
            MaxRetries = 2;
            NeedPubSubPingSeconds = 10;
            PubSubPollEveryMilliseconds = 4000;
            FirstPubSubPollAfterMilliseconds = 500;
            PubSubKeepAliveEveryMilliseconds = 2000;
            RetryAfterSeconds = 1;
            MaxDeliveryCount = 5;
            ReceivePauseOnNoPendingMilliseconds = 200;
        }
    }
}
