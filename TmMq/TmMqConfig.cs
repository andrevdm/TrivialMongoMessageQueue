namespace TmMq
{
    public class TmMqConfig
    {
        public TmMqConfig()
        {
            MaxRetries = 3;
            NeedPubSubPingSeconds = 10;
            PubSubPollEveryMilliseconds = 4000;
            FirstPubSubPollAfterMilliseconds = 500;
            PubSubKeepAliveEveryMilliseconds = 2000;
            RetryAfterSeconds = 2;
            MaxDeliveryCount = 5;
            ReceivePauseOnNoPendingMilliseconds = 800;
        }

        public int MaxRetries { get; set; }
        public int MaxDeliveryCount { get; set; }
        public int RetryAfterSeconds { get; set; }
        public int NeedPubSubPingSeconds { get; set; }
        public int PubSubPollEveryMilliseconds { get; set; }
        public int FirstPubSubPollAfterMilliseconds { get; set; }
        public int PubSubKeepAliveEveryMilliseconds { get; set; }
        public int ReceivePauseOnNoPendingMilliseconds { get; set; }
    }
}
