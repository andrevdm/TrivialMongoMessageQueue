using System;
using MongoDB.Driver;

namespace TmMq
{
    public class TmMqPubSubSender : TmMqSender
    {
        protected override MongoCollection MessagesCollection { get { return null; } }

        public TmMqPubSubSender( string queueName )
            : base( queueName )
        {
        }

        protected override MongoCollection CreateMessagesCollection()
        {
            return null;
        }

        public override ITmMqMessage Send( ITmMqMessage message )
        {           
            #region param checks
            if( message == null )
            {
                throw new ArgumentNullException( "message" );
            }
            #endregion

            DistributestringMessage( MongoDbQueueName, message );
            return message;
        }
    }
}