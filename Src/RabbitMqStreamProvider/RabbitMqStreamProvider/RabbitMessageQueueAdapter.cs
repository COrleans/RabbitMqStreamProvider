using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Streams;

namespace Orleans.RabbitMqStreamProvider
{
    internal class RabbitMessageQueueAdapter : IQueueAdapter
    {
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;
        public bool IsRewindable => false;
        public string Name { get; private set; }
        private HashRingBasedStreamQueueMapper StreamQueueMapper { get; set; }
        protected readonly ConcurrentDictionary<QueueId, RabbitMessageQueueDataManager> Queues = new ConcurrentDictionary<QueueId, RabbitMessageQueueDataManager>();

        private string RabbitServerConnString { get; set; }
        private string DeploymentId { get; set; }

        public RabbitMessageQueueAdapter(string providerName, HashRingBasedStreamQueueMapper streamQueueMapper, string rabbitServerConnString, string deploymentId = "")
        {
            if (string.IsNullOrEmpty(rabbitServerConnString))
                throw new ArgumentNullException(nameof(rabbitServerConnString));

            this.Name = providerName;
            this.StreamQueueMapper = streamQueueMapper;
            this.RabbitServerConnString = rabbitServerConnString;
            this.DeploymentId = deploymentId;
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return RabbitMessageQueueAdapterReceiver.Create(queueId, this.RabbitServerConnString, this.DeploymentId);
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            var queueId = StreamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            RabbitMessageQueueDataManager queue;
            if (!Queues.TryGetValue(queueId, out queue))
            {
                var tmpQueue = new RabbitMessageQueueDataManager(queueId.ToString(), DeploymentId, RabbitServerConnString);
                await tmpQueue.InitQueueAsync();
                queue = Queues.GetOrAdd(queueId, tmpQueue);
            }
            var cloudMsg = RabbitMessageQueueBatchContainer.ToRabbitMessageQueueMessage(streamGuid, streamNamespace, events, requestContext);
            await queue.AddQueueMessage(cloudMsg);
        }
    }
}
