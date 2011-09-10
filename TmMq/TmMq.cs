using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace TmMq
{
    public abstract class TmMq : IDisposable
    {
        protected MongoServer m_server;
        protected readonly MongoDatabase m_db;
        private readonly ConcurrentDictionary<string, MongoCollection> m_collections = new ConcurrentDictionary<string, MongoCollection>();
        protected MongoCollection ErrorCollection { get; private set; }
        protected abstract MongoCollection MessagesCollection { get; }

        private static object m_pubsubUpdateSync = new object();
        private static bool g_pubsubUpdating;
        private static readonly Timer g_pubsubUpdateTimer;
        private static readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, DateTime>> g_pubsubQueues = new ConcurrentDictionary<string, ConcurrentDictionary<Guid, DateTime>>();
        private static readonly int g_needPubSubPingSeconds = 10;//TODO configure pubsub timeout
        private readonly MongoCollection m_configCollection;

        static TmMq()
        {
            BsonSerializer.RegisterSerializer( typeof( DynamicDictionary ), new DynamicDictionarySerializer() );

            BsonClassMap.RegisterClassMap<DynamicDictionary>( cm =>
            {
                cm.AutoMap();
                cm.SetDiscriminator( "DynamicDictionary" );
            } );

            g_pubsubUpdateTimer = new Timer( MonitorPubSubConfig, null, 15000, 4000 ); //TODO configure poll period
        }

        protected TmMq()
        {
            m_server = CreateMongoDbServer();
            m_db = m_server["tmmq"];
            ErrorCollection = GetCollection( "error" );
            m_configCollection = GetCollection( "config" );

            m_configCollection.Update( Query.EQ( "Type", "pubsub" ), Update.Set( "Type", "pubsub" ), UpdateFlags.Upsert, SafeMode.True );

            //Find orphaned pub/sub collections
            lock( m_pubsubUpdateSync )
            {
                g_pubsubUpdating = true;
                try
                {
                    foreach( var name in m_db.GetCollectionNames() )
                    {
                        if( name.StartsWith( "pubsub_" ) )
                        {
                            int idxSubscriber = name.LastIndexOf( '_' );

                            if( idxSubscriber < 7 )
                            {
                                continue;
                            }

                            string queueName = name.Substring( 7, idxSubscriber - 7 );
                            string subscriberName = name.Substring( idxSubscriber + 1 );

                            ConcurrentDictionary<Guid, DateTime> subscribers;

                            if( !g_pubsubQueues.TryGetValue( queueName, out subscribers ) )
                            {
                                subscribers = new ConcurrentDictionary<Guid, DateTime>();
                                g_pubsubQueues[queueName] = subscribers;
                            }

                            subscribers[Guid.Parse( subscriberName )] = DateTime.MinValue;
                        }
                    }
                }
                finally
                {
                    g_pubsubUpdating = false;
                }
            }
        }

        public MongoServer CreateMongoDbServer()
        {
            string con = ConfigurationManager.AppSettings["MongoDB.Server"];
            return MongoServer.Create( con );
        }

        public ITmMqMessage CreateMessage()
        {
            return new TmMqMessage();
        }

        public void Acknowledge( ITmMqMessage msg )
        {
            MessagesCollection.Remove( Query.EQ( "MessageId", msg.MessageId ), SafeMode.False );
        }

        public void Fail( ITmMqMessage msg, Exception exception )
        {
            MessagesCollection.Update(
                         Query.EQ( "MessageId", msg.MessageId ),
                         Update.Inc( "RetryCount", 1 ) );

            MessagesCollection.Update(
                         Query.EQ( "MessageId", msg.MessageId ),
                         Update.PushWrapped( "Errors", new TmMqMessageError( exception.ToString() ) ) );
        }

        public MongoCollection GetCollection( string name )
        {
            #region param checks
            if( string.IsNullOrWhiteSpace( name ) )
            {
                throw new ArgumentException( "name must be specified", "name" );
            }
            #endregion

            MongoCollection col;

            if( !m_collections.TryGetValue( name, out col ) )
            {
                if( !m_db.CollectionExists( name ) )
                {
                    col = m_db[name];
                    col.EnsureIndex( new[] { "TimeStamp" } );
                    col.EnsureIndex( new[] { "DeliveredAt" } );
                    col.EnsureIndex( new[] { "HoldUntil" } );
                    col.EnsureIndex( new[] { "ExpireAt" } );
                    col.EnsureIndex( new[] { "MessageId" } );
                    m_collections[name] = col;
                }
                else
                {
                    col = m_db[name];
                }
            }

            return col;
        }

        private static void MonitorPubSubConfig( object state )
        {
            if( g_pubsubUpdating )
            {
                return;
            }

            g_pubsubUpdating = true;

            try
            {
                string con = ConfigurationManager.AppSettings["MongoDB.Server"];
                var server = MongoServer.Create( con );
                var db = server["tmmq"];
                var configCollection = db["config"];

                var config = configCollection.FindOne( Query.EQ( "Type", "pubsub" ) );

                if( config == null )
                {
                    return;
                }

                lock( m_pubsubUpdateSync )
                {
                    foreach( var element in config )
                    {
                        string name = element.Name;

                        if( !name.StartsWith( "pubsub_" ) )
                        {
                            continue;
                        }

                        int idxSubscriber = name.LastIndexOf( '_' );

                        if( idxSubscriber < 7 )
                        {
                            continue;
                        }

                        string queueName = name.Substring( 7, idxSubscriber - 7 );
                        string subscriberName = name.Substring( idxSubscriber + 1 );

                        DateTime at = element.Value.AsDateTime;

                        if( (DateTime.UtcNow - at).TotalSeconds > g_needPubSubPingSeconds )
                        {
                            ConcurrentDictionary<Guid, DateTime> x;
                            g_pubsubQueues.TryRemove( name.Substring( 7 ), out x );

                            configCollection.Update( Query.EQ( "Type", "pubsub" ), Update.Unset( name ) );

                            if( db.CollectionExists( name ) )
                            {
                                db.DropCollection( name );
                            }
                        }
                        else
                        {
                            ConcurrentDictionary<Guid, DateTime> subscribers;

                            if( !g_pubsubQueues.TryGetValue( queueName, out subscribers ) )
                            {
                                subscribers = new ConcurrentDictionary<Guid, DateTime>();
                                g_pubsubQueues[queueName] = subscribers;
                            }

                            subscribers[Guid.Parse( subscriberName )] = at;
                        }
                    }

                    foreach( var queueName in g_pubsubQueues.Keys )
                    {
                        ConcurrentDictionary<Guid, DateTime> queue;

                        if( g_pubsubQueues.TryGetValue( queueName, out queue ) )
                        {
                            foreach( var last in queue )
                            {
                                if( (DateTime.UtcNow - last.Value).TotalSeconds > g_needPubSubPingSeconds )
                                {
                                    DateTime x;
                                    queue.TryRemove( last.Key, out x );

                                    if( db.CollectionExists( "pubsub_" + queueName + "_" + last.Key ) )
                                    {
                                        db.DropCollection( "pubsub_" + queueName + "_" + last.Key );
                                    }
                                }
                            }
                        }
                    }
                }
            }
            finally
            {
                g_pubsubUpdating = false;
            }
        }

        private static void MonitorPubSubConfig_TailableCursor()
        {
            string con = ConfigurationManager.AppSettings["MongoDB.Server"];
            var server = MongoServer.Create( con );
            var db = server["local"];
            var oplog = db["oplog.rs"];
            BsonTimestamp lastTs = BsonTimestamp.Create( 0 );

            while( true )
            {
                var query = Query.GT( "ts", lastTs );

                var cursor = oplog.Find( query )
                        .SetFlags( QueryFlags.TailableCursor | QueryFlags.AwaitData )
                        .SetSortOrder( "$natural" );

                using( var enumerator = (MongoCursorEnumerator<BsonDocument>)cursor.GetEnumerator() )
                {
                    while( true )
                    {
                        if( enumerator.MoveNext() )
                        {
                            var document = enumerator.Current;
                            lastTs = document["ts"].AsBsonTimestamp;

                            Console.WriteLine( document.GetValue( "op" ) );
                            //ProcessDocument(document);
                        }
                        else
                        {
                            if( enumerator.IsDead )
                            {
                                break;
                            }
                            if( !enumerator.IsServerAwaitCapable )
                            {
                                Thread.Sleep( 100 );
                            }
                        }
                    }
                }
            }
        }

        protected void KeepAlive( string pubsubQueueName, Guid subscriberName )
        {
            #region param checks
            if( pubsubQueueName == null )
            {
                throw new ArgumentNullException( "pubsubQueueName" );
            }
            #endregion

            lock( m_pubsubUpdateSync )
            {
                ConcurrentDictionary<Guid, DateTime> subscribers;

                if( !g_pubsubQueues.TryGetValue( pubsubQueueName, out subscribers ) )
                {
                    subscribers = new ConcurrentDictionary<Guid, DateTime>();
                    g_pubsubQueues[pubsubQueueName] = subscribers;
                }

                subscribers[subscriberName] = DateTime.UtcNow;

                m_configCollection.Update( Query.EQ( "Type", "pubsub" ), Update.Set( "pubsub_" + pubsubQueueName + "_" + subscriberName, DateTime.UtcNow ), UpdateFlags.Upsert );
            }
        }

        protected void DistributestringMessage( string pubsubQueueName, ITmMqMessage message )
        {
            #region param checks
            if( pubsubQueueName == null )
            {
                throw new ArgumentNullException( "pubsubQueueName" );
            }

            if( message == null )
            {
                throw new ArgumentNullException( "message" );
            }
            #endregion

            ConcurrentDictionary<Guid, DateTime> subscribers;

            if( g_pubsubQueues.TryGetValue( pubsubQueueName, out subscribers ) )
            {
                foreach( var subscriber in subscribers.Keys )
                {
                    var col = GetCollection( "pubsub_" + pubsubQueueName + "_" + subscriber );
                    col.Insert( message );
                }
            }
        }

        public virtual void Dispose()
        {
            if( m_server != null )
            {
                m_server.Disconnect();
                m_server = null;
            }
        }
    }
}