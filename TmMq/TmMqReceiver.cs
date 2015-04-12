using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace TmMq
{
    public class TmMqReceiver : TmMq
    {
        private bool m_active = true;
        private Task[] m_receivingPool;
        private readonly Lazy<MongoCollection> m_messagesCollection;
        private readonly object m_syncReceivingPool = new object();

        public string MongoDbQueueName { get; private set; }
        public string QueueName { get; private set; }
        protected override MongoCollection MessagesCollection { get { return m_messagesCollection.Value; } }

        public TmMqReceiver( string queueName )
        {
            #region param checks
            if( string.IsNullOrWhiteSpace( queueName ) )
            {
                throw new ArgumentException( "queueName name must be specified", "queueName" );
            }
            #endregion

            QueueName = queueName;
            MongoDbQueueName = queueName.Replace( ".", "~" );
            m_messagesCollection = new Lazy<MongoCollection>( CreateMessagesCollection, true );
        }

        protected virtual MongoCollection CreateMessagesCollection()
        {
            return GetCollection( MongoDbQueueName );
        }

        protected virtual IMongoQuery CreateActiveItemsQuery()
        {
            var query = Query.And(
                Query.LTE( "DeliveredAt", DateTime.UtcNow.AddSeconds( -g_config.RetryAfterSeconds ) ),
                Query.Or(
                    Query.EQ( "HoldUntil", BsonNull.Value ),
                    Query.LT( "HoldUntil", DateTime.UtcNow ) )
                );

            return query;
        }

        public virtual long CountPending()
        {
            return MessagesCollection.Count();
        }

        public virtual IEnumerable<ITmMqMessage> Receive( CancellationToken? cancelToken = null )
        {
            while( m_active )
            {
                FindAndModifyResult found = null;

                if( cancelToken.HasValue && cancelToken.Value.IsCancellationRequested )
                {
                    yield break;
                }

                try
                {
                    var skip = false;

#if DEBUG
                    //Try to avoid exception in old versions on MongoDB while debugging
                    if( System.Diagnostics.Debugger.IsAttached )
                    {
                        if( MessagesCollection.FindOneAs<BsonDocument>( CreateActiveItemsQuery() ) == null )
                        {
                            skip = true;
                        }
                    }
#endif

                    if( !skip )
                    {
                        found = MessagesCollection.FindAndModify(
                                                                   CreateActiveItemsQuery(),
                                                                   SortBy.Ascending( "TimeStamp" ),
                                                                   Update.Set( "DeliveredAt", DateTime.UtcNow ),
                                                                   false );
                    }
                }
                catch( MongoCommandException ex )
                {
                    //Horrible hack. In mongo driver version prior to 1.8 this exception is not suppresed in the drivers. In mongodb version 2.0 this will be suppresed at the server
                    // This is here for backward capability.
                    if( !ex.Message.StartsWith( "Command 'findAndModify' failed: No matching object found" ) )
                    {
                        throw;
                    }
                }

                if( found != null && found.ModifiedDocument != null )
                {
                    var msg = (TmMqMessage)BsonSerializer.Deserialize( found.ModifiedDocument, typeof( TmMqMessage ) );

                    if( (MongoDbQueueName != "error") && (msg.ExpireAt != null) && (msg.ExpireAt < DateTime.UtcNow) )
                    {
                        Acknowledge( msg );
                        continue;
                    }

                    if( (MongoDbQueueName != "error") && (msg.RetryCount >= g_config.MaxRetries) ) 
                    {
                        MoveToErrorQueue( msg );
                        continue;
                    }

                    if( msg.DeliveryCount > g_config.MaxDeliveryCount ) 
                    {
                        Acknowledge( msg );
                        continue;
                    }

                    MessagesCollection.Update(
                                 Query.EQ( "MessageId", msg.MessageId ),
                                 Update.Inc( "DeliveryCount", 1 ) );

                    yield return msg;
                }
                else
                {
                    if( cancelToken.HasValue && cancelToken.Value.IsCancellationRequested )
                    {
                        yield break;
                    }

                    Thread.Sleep( g_config.ReceivePauseOnNoPendingMilliseconds );
                }
            }
        }


        public void StartReceiving( int workerPoolSize, Action<ITmMqMessage> act, CancellationToken? cancelToken = null )
        {
            if( m_receivingPool != null || ( cancelToken.HasValue && cancelToken.Value.IsCancellationRequested ) )
            {
                return;
            }

            lock( m_syncReceivingPool )
            {
                if( m_receivingPool != null )
                {
                    return;
                }

                m_receivingPool = new Task[workerPoolSize];
            }

            for( int i = 0; i < workerPoolSize; ++i )
            {
                m_receivingPool[i] = Task.Factory.StartNew( () => Receive( act, cancelToken ) );
            }
        }

        private void MoveToErrorQueue( TmMqMessage msg )
        {
            MessagesCollection.Remove( Query.EQ( "MessageId", msg.MessageId ), WriteConcern.Acknowledged );

            msg.OriginalQueue = MongoDbQueueName;
            ErrorCollection.Insert( new TmMqMessage( msg ), WriteConcern.Acknowledged );
        }

        private void ActOnMessages( Action<ITmMqMessage> act, CancellationToken? cancelToken = null )
        {
            foreach( var msg in Receive( cancelToken ) )
            {
                try
                {
                    act( msg );
                    Acknowledge( msg );
                }
                catch( Exception ex )
                {
                    Fail( msg, ex );
                }
            }
        }

        public virtual void Receive( Action<ITmMqMessage> act, CancellationToken? cancelToken = null )
        {
            ActOnMessages( act, cancelToken );
        }

        public override void Dispose()
        {
            m_active = false;

            base.Dispose();
        }

        public bool SupportsAsyncNumberPendingQuery { get { return true; } }
        public bool SupportsSyncNumberPendingQuery { get { return true; } }

        public long GetNumberOfPendingMessages()
        {
            return CountPending();
        }

        public void GetNumberOfPendingMessages( Action<long> continueWith, TimeSpan timeout )
        {
            continueWith( GetNumberOfPendingMessages() );
        }
    }
}