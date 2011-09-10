using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using MongoDB.Driver;

namespace TmMq
{
    public class TmMqPubSubReceiver : TmMqReceiver
    {
        private Timer m_timer;
        private readonly Guid m_subscriberId = Guid.NewGuid();

        public TmMqPubSubReceiver( string queueName )
            : base( queueName )
        {
            KeepAlive( null );
            m_timer = new Timer( KeepAlive, null, 2000, 2000 ); //TODO configure keepalive seconds
        }

        protected override MongoCollection CreateMessagesCollection()
        {
            return GetCollection( "pubsub_" + MongoDbQueueName + "_" + m_subscriberId );
        }

        private void KeepAlive( object state )
        {
            KeepAlive( MongoDbQueueName, m_subscriberId );
        }

        public override void Dispose()
        {
            if( m_timer != null )
            {
                m_timer.Dispose();
                m_timer = null;
            }

            base.Dispose();
        }
    }
}
