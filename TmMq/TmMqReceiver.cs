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
        private readonly MongoCollection m_messagesCollection;
        private readonly object m_syncReceivingPool = new object();

        public string MongoDbQueueName { get; private set; }
        public string QueueName { get; private set; }
        protected override MongoCollection MessagesCollection { get { return m_messagesCollection; } }

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
            m_messagesCollection = CreateMessagesCollection();
        }

        protected virtual MongoCollection CreateMessagesCollection()
        {
            return GetCollection( MongoDbQueueName );
        }

        protected virtual QueryComplete CreateActiveItemsQuery()
        {
            QueryComplete query = Query.And(
                                            Query.LTE( "DeliveredAt", DateTime.UtcNow.AddSeconds( -2 ) ), //TODO retry after must be configurable
                                            Query.Or(
                                                     Query.EQ( "HoldUntil", BsonNull.Value ),
                                                     Query.LT( "HoldUntil", DateTime.UtcNow ) )
                );

            return query;
        }

        public virtual int CountPending()
        {
            return m_messagesCollection.Count();
        }

        public virtual IEnumerable<ITmMqMessage> Receive()
        {
            while( m_active )
            {
                FindAndModifyResult found = null;

                try
                {
                    bool skip = false;

#if DEBUG
                    //Try to avoid exception in old versions on MongoDB while debugging
                    if( System.Diagnostics.Debugger.IsAttached )
                    {
                        if( m_messagesCollection.FindOneAs<BsonDocument>( CreateActiveItemsQuery() ) == null )
                        {
                            skip = true;
                        }
                    }
#endif

                    if( !skip )
                    {
                        found = m_messagesCollection.FindAndModify(
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

                    if( (msg.ExpireAt != null) && (msg.ExpireAt < DateTime.UtcNow) )
                    {
                        Acknowledge( msg );
                        continue;
                    }

                    if( msg.RetryCount > 3 ) //TODO configure retry count
                    {
                        MoveToErrorQueue( msg );
                        continue;
                    }

                    if( msg.DeliveryCount > 5 ) //TODO configure retry count
                    {
                        MoveToErrorQueue( msg );
                        continue;
                    }

                    m_messagesCollection.Update(
                                 Query.EQ( "MessageId", msg.MessageId ),
                                 Update.Inc( "DeliveryCount", 1 ) );

                    yield return msg;
                }
                else
                {
                    Thread.Sleep( 800 ); //TODO configurable
                }
            }
        }

        public void StartReceiving( int workerPoolSize, Action<ITmMqMessage> act )
        {
            if( m_receivingPool != null )
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
                m_receivingPool[i] = Task.Factory.StartNew( () => Receive( act ) );
            }
        }

        private void MoveToErrorQueue( TmMqMessage msg )
        {
            m_messagesCollection.Remove( Query.EQ( "MessageId", msg.MessageId ), SafeMode.True );

            msg.OriginalQueue = MongoDbQueueName;
            ErrorCollection.Insert( new TmMqMessage( msg ), SafeMode.True );
        }

        private void ActOnMessages( Action<ITmMqMessage> act )
        {
            foreach( var msg in Receive() )
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

        public virtual void Receive( Action<ITmMqMessage> act )
        {
            ActOnMessages( act );
        }

        public override void Dispose()
        {
            m_active = false;

            base.Dispose();
        }

        public bool SupportsAsyncNumberPendingQuery { get { return true; } }
        public bool SupportsSyncNumberPendingQuery { get { return true; } }

        public int GetNumberOfPendingMessages()
        {
            return CountPending();
        }

        public void GetNumberOfPendingMessages( Action<int> continueWith, TimeSpan timeout )
        {
            continueWith( GetNumberOfPendingMessages() );
        }
    }
}