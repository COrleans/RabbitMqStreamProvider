using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Streams;

namespace Orleans.RabbitMqStreamProvider
{
    internal class RabbitMessageQueueAdapterReceiver : IQueueAdapterReceiver
    {
        private RabbitMessageQueueDataManager _queue;
        private long _lastReadMessage;
        private Task _outstandingTask;
        private const int MaxNumberOfMessagesToPeek = 32;
        private QueueId Id { get; set; }

        public static IQueueAdapterReceiver Create(QueueId queueId, string rabbitMqConnectionString, string deploymentId = "")
        {
            if (queueId == null) throw new ArgumentNullException(nameof(queueId));
            if (String.IsNullOrEmpty(rabbitMqConnectionString)) throw new ArgumentNullException(nameof(rabbitMqConnectionString));

            var queue = new RabbitMessageQueueDataManager(queueId.ToString(), deploymentId, rabbitMqConnectionString);

            return new RabbitMessageQueueAdapterReceiver(queueId, queue);
        }

        private RabbitMessageQueueAdapterReceiver(QueueId queueId, RabbitMessageQueueDataManager queue)
        {
            if (queueId == null) throw new ArgumentNullException(nameof(queueId));
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            this.Id = queueId;
            this._queue = queue;
        }
        public Task Initialize(TimeSpan timeout)
        {
            return _queue.InitQueueAsync();
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            try
            {
                var queueRef = _queue; // store direct ref, in case we are somehow asked to shutdown while we are receiving.    
                if (queueRef == null) return new List<IBatchContainer>();
                var count = maxCount < 0 || maxCount == QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG ?
                    MaxNumberOfMessagesToPeek : Math.Min(maxCount, MaxNumberOfMessagesToPeek);
                var task = queueRef.GetQueueMessagesAsync(count);
                _outstandingTask = task;
                var messages = await task;
                var azureQueueMessages = messages
                    .Select(msg => (IBatchContainer)RabbitMessageQueueBatchContainer.FromRabbitMessageQueueMessage(msg, _lastReadMessage++)).ToList();
                return azureQueueMessages;
            }
            finally
            {
                _outstandingTask = null;
            }
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            try
            {
                var queueRef = _queue; // store direct ref, in case we are somehow asked to shutdown while we are receiving.  
                if (messages.Count == 0 || queueRef == null)
                    return;
                var cloudQueueMessages = messages.Cast<RabbitMessageQueueBatchContainer>().Select(b => b.QueueMessage).ToList();
                _outstandingTask = Task.WhenAll(cloudQueueMessages.Select(queueRef.DeleteQueueMessage));
                await _outstandingTask;
            }
            finally
            {
                _outstandingTask = null;
            }
        }

        public async Task Shutdown(TimeSpan timeout)
        {
            try
            {
                if (_outstandingTask != null)
                    await _outstandingTask;
            }
            finally
            {
                _queue = null;  // remember that we shut down so we never try to read from the queue again.
            }
        }
    }
}
