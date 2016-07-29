using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.RabbitMqStreamProvider
{
    [Serializable]
    class RabbitMessageQueueBatchContainer : IBatchContainer
    {
        [JsonProperty]
        private EventSequenceToken _sequenceToken;

        [JsonProperty]
        private readonly List<object> _events;

        [JsonProperty]
        private readonly Dictionary<string, object> _requestContext;

        [NonSerialized]
        internal RabbitMessage QueueMessage;

        public Guid StreamGuid { get; private set; }
        public string StreamNamespace { get; private set; }

        public StreamSequenceToken SequenceToken
        {
            get { return _sequenceToken; }
        }

        [JsonConstructor]
        private RabbitMessageQueueBatchContainer(
                   Guid streamGuid,
                   String streamNamespace,
                   List<object> events,
                   Dictionary<string, object> requestContext,
                   EventSequenceToken sequenceToken)
            : this(streamGuid, streamNamespace, events, requestContext)
        {
            this._sequenceToken = sequenceToken;
        }

        private RabbitMessageQueueBatchContainer(Guid streamGuid, string streamNamespace, List<object> events, Dictionary<string, object> requestContext)
        {
            if (events == null)
                throw new ArgumentNullException(nameof(events), "Message contains no events");

            StreamGuid = streamGuid;
            StreamNamespace = streamNamespace;
            _events = events;
            _requestContext = requestContext;
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            return _events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, _sequenceToken.CreateSequenceTokenForEvent(i)));
        }

        public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
        {
            // There is something in this batch that the consumer is intereted in, so we should send it.
            // Consumer is not interested in any of these events, so don't send.
            return _events.Any(item => shouldReceiveFunc(stream, filterData, item));
        }

        internal static RabbitMessage ToRabbitMessageQueueMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            var rabbitQueueBatchMessage = new RabbitMessageQueueBatchContainer(streamGuid, streamNamespace, events.Cast<object>().ToList(), requestContext);
            var rawBytes = SerializationManager.SerializeToByteArray(rabbitQueueBatchMessage);
            return new RabbitMessage(rawBytes);
        }

        public bool ImportRequestContext()
        {
            if (_requestContext != null)
            {
                RequestContext.Import(_requestContext);
                return true;
            }
            return false;
        }

        internal static RabbitMessageQueueBatchContainer FromRabbitMessageQueueMessage(RabbitMessage cloudMsg, long sequenceId)
        {
            var rabbitQueueBatch = SerializationManager.DeserializeFromByteArray<RabbitMessageQueueBatchContainer>(cloudMsg.Body);
            rabbitQueueBatch.QueueMessage = cloudMsg;
            rabbitQueueBatch._sequenceToken = new EventSequenceToken(sequenceId);
            return rabbitQueueBatch;
        }

        public override string ToString()
        {
            return $"[RabbitMessageQueueBatchContainer:Stream={StreamGuid},#Items={_events.Count}]";
        }
    }
}
