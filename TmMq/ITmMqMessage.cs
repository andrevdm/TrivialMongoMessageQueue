using System;
using System.Collections.Generic;

namespace TmMq
{
    public interface ITmMqMessage
    {
        Guid MessageId { get; }
        DateTime TimeStamp { get; }
        DateTime DeliveredAt { get; }
        Guid CorrelationId { get; }
        int RetryCount { get; }
        int DeliveryCount { get; }
        string Type { get; set; }
        dynamic Properties { get; }
        List<TmMqMessageError> Errors { get; }
        string ReplyTo { get; set; }
        string Text { get; set; }
        DateTime? ExpireAt { get; set; }
        DateTime? HoldUntil { get; set; }
        string OriginalQueue { get; set; }
    }
}