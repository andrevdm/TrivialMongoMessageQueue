using System;
using MongoDB.Driver;

namespace TmMq
{
    public class TmMqSender : TmMq
    {
        private readonly MongoCollection m_messagesCollection;

        public string MongoDbQueueName { get; private set; }
        public string QueueName { get; private set; }
        protected override MongoCollection MessagesCollection { get { return m_messagesCollection; } }

        public TmMqSender( string queueName )
        {
            #region param checks
            if( string.IsNullOrWhiteSpace( queueName ) )
            {
                throw new ArgumentException( "queueName must be specified", "queueName" );
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

        public virtual ITmMqMessage Send( ITmMqMessage message )
        {
            #region param checks
            if( message == null )
            {
                throw new ArgumentNullException( "message" );
            }
            #endregion

            m_messagesCollection.Insert( message, SafeMode.True );
            return message;
        }

        public virtual void Send( ITmMqMessage message, bool safeSend )
        {
            #region param checks
            if( message == null )
            {
                throw new ArgumentNullException( "message" );
            }
            #endregion

            m_messagesCollection.Insert( message, safeSend ? SafeMode.True : SafeMode.False );
        }
    }
}