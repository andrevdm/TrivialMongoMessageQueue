TmMq
=====

TmMq - Trivial MongoDB Message Queue is a very simple .net message queuing system built on MongoDB

It is not in any way meant to compete with any of the fully fledged messaging solutions (Hortet, ActiveMQ etc) but it is a nice, lightweight alternative that has proved useful to me.


Features
--------

1. No TmMq server
2. Send & receive
3. Publish / subscribe
4. Redeliver on error with limit on retry
5. Limit on delivery (at-least-once delivery)
6. Message expiry
7. Message holding (only deliver in future)
8. Errors logged in message
9. Dynamic properties collection
10. Synchronous and asynchronous receive
11. Written in C#


TODO
-----
1. Triggers based on tailable MongoDB cursor. I'm not sure this is necessary, I will implement it if I find I need it.
2. More unit tests


Licence
--------
FreeBSD License. See licence.txt


Usage
------

See the unit tests for examples of all the features including pub/sub, retry, errors etc.


#### Send & receive
	using( var send = new TmMqSender( "TestSendBeforeReceiveStarted" ) )
	{
		 var msg = new TmMqMessage();
		 msg.Text = "msg1";
		 send.Send( msg );
	}

	using( var recv = new TmMqReceiver( "TestSendBeforeReceiveStarted" ) )
	{
		 ITmMqMessage recieved = recv.Receive().FirstOrDefault();
	}


#### Pub/sub
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
				var msg = new TmMqMessage();
				msg.Text = "ps-" + i;
				sender.Send( msg );
		 }
