using System;
using System.Threading.Tasks;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Providers.Streams.Generator;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.RabbitMqStreamProvider
{
    public class RabbitMessageQueueAdapterFactory : IQueueAdapterFactory
    {
        private const string CACHE_SIZE_PARAM = "CacheSize";
        private const int DEFAULT_CACHE_SIZE = 4096;
        private const string NUM_QUEUES_PARAM = "NumQueues";
        public const int DEFAULT_NUM_QUEUES = 8; // keep as power of 2.

        private string _deploymentId;
        private string _dataConnectionString;
        private string _providerName;
        private int _cacheSize;
        private int _numQueues;
        private HashRingBasedStreamQueueMapper _streamQueueMapper;
        private IQueueAdapterCache _adapterCache;
        /// <summary>"DataConnectionString".</summary>
        public const string RABBITMQ_CONNECTION_STRING = "RabbitMqConnectionString";
        /// <summary>"DeploymentId".</summary>
        public const string DEPLOYMENT_ID = "DeploymentId";
        protected Func<QueueId, Task<IStreamFailureHandler>> StreamFailureHandlerFactory { private get; set; }


        public void Init(IProviderConfiguration config, string providerName, Logger logger, IServiceProvider serviceProvider)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (!config.Properties.TryGetValue(RABBITMQ_CONNECTION_STRING, out _dataConnectionString))
                throw new ArgumentException($"{RABBITMQ_CONNECTION_STRING} property not set");
            if (!config.Properties.TryGetValue(DEPLOYMENT_ID, out _deploymentId))
                throw new ArgumentException($"{DEPLOYMENT_ID} property not set");
            string cacheSizeString;
            _cacheSize = DEFAULT_CACHE_SIZE;
            if (config.Properties.TryGetValue(CACHE_SIZE_PARAM, out cacheSizeString))
                if (!int.TryParse(cacheSizeString, out _cacheSize))
                    throw new ArgumentException($"{CACHE_SIZE_PARAM} invalid.  Must be int");
            string numQueuesString;
            _numQueues = DEFAULT_NUM_QUEUES;
            if (config.Properties.TryGetValue(NUM_QUEUES_PARAM, out numQueuesString))
                if (!int.TryParse(numQueuesString, out _numQueues))
                    throw new ArgumentException($"{NUM_QUEUES_PARAM} invalid.  Must be int");
            _providerName = providerName;
            _streamQueueMapper = new HashRingBasedStreamQueueMapper(_numQueues, providerName);
            _adapterCache = new SimpleQueueAdapterCache(_cacheSize, logger);
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new RabbitMessageQueueAdapter(_providerName, _streamQueueMapper, _dataConnectionString, _deploymentId);

            return Task.FromResult<IQueueAdapter>(adapter);
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return _adapterCache;
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return _streamQueueMapper;
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return StreamFailureHandlerFactory(queueId);
        }
    }
}
