using System;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace TmMq
{
    [Serializable]
    public class TmMqMessage : ITmMqMessage
    {
        public TmMqMessage()
        {
            MessageId = Guid.NewGuid();
            Properties = new DynamicDictionary();
            Errors = new List<TmMqMessageError>();
            TimeStamp = DateTime.UtcNow;
            DeliveredAt = new DateTime( 2010, 01, 01, 0, 0, 0, DateTimeKind.Utc );
            ExpireAt = DateTime.UtcNow.AddDays( 7 );
        }

        public TmMqMessage( ITmMqMessage copy )
        {
            #region param checks
            if( copy == null )
            {
                throw new ArgumentNullException( "copy" );
            }
            #endregion

            MessageId = copy.MessageId;
            Properties = new DynamicDictionary( copy.Properties );
            Errors = new List<TmMqMessageError>( copy.Errors );
            TimeStamp = copy.TimeStamp;
            DeliveredAt = copy.DeliveredAt;

            CorrelationId = copy.CorrelationId;
            RetryCount = copy.RetryCount;
            DeliveryCount = copy.DeliveryCount;
            Type = copy.Type;
            OriginalQueue = copy.OriginalQueue;
            ReplyTo = copy.ReplyTo;
            Text = copy.Text;
            ExpireAt = copy.ExpireAt;
            HoldUntil = copy.HoldUntil;
        }

        public ObjectId Id { get; private set; }
        [BsonRepresentation( BsonType.Binary )]
        public Guid MessageId { get; private set; }
        public DateTime TimeStamp { get; private set; }
        public DateTime DeliveredAt { get; private set; }
        [BsonRepresentation( BsonType.Binary )]
        public Guid CorrelationId { get; private set; }
        public int RetryCount { get; private set; }
        public int DeliveryCount { get; private set; }
        public string Type { get; set; }
        public dynamic Properties { get; private set; }
        public List<TmMqMessageError> Errors { get; private set; }
        public string OriginalQueue { get; set; }

        public string ReplyTo { get; set; }
        public string Text { get; set; }
        public DateTime? ExpireAt { get; set; }
        public DateTime? HoldUntil { get; set; }
    }
}