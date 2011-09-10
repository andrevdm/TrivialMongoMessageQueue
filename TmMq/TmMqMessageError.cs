using System;

namespace TmMq
{
    [Serializable]
    public class TmMqMessageError
    {
        public TmMqMessageError( string error )
        {
            At = DateTime.Now;
            Error = error;
        }

        public DateTime At { get; private set; }
        public string Error { get; set; }
    }
}