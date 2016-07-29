using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Streams;
using RabbitMQ.Client;

namespace Orleans.RabbitMqStreamProvider
{
    internal class RabbitMessageQueueDataManager
    {
        public string QueueName { get; private set; }
        private readonly IBasicProperties _commonMessageHeader;
        private IModel _channel;

        public RabbitMessageQueueDataManager(string queueName, string rabbitServerConnString)
        {
            if (string.IsNullOrEmpty(rabbitServerConnString))
                throw new ArgumentNullException(nameof(rabbitServerConnString));
            if (string.IsNullOrEmpty(queueName))
                throw new ArgumentNullException(nameof(queueName));

            this.QueueName = queueName;
            this._channel = RabbitMessageQueueUtils.GetQueueChanel(rabbitServerConnString);
            this._commonMessageHeader = this.GetCommonMessageHeader();
        }
        public RabbitMessageQueueDataManager(string queueName, string deploymentId, string rabbitServerConnString)
        {
            this.QueueName = string.Concat(deploymentId, "-", queueName);
            this._channel = RabbitMessageQueueUtils.GetQueueChanel(rabbitServerConnString);
            this._commonMessageHeader = GetCommonMessageHeader();
        } 

        /// <summary>
        /// Initializes the queue.
        /// </summary>
        public Task InitQueueAsync()
        {
            var startTime = DateTime.UtcNow;

            try
            {
                _channel.QueueDeclare(QueueName, true, false, false, null);
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "CreateIfNotExist");
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "InitQueue_Async");
            }
            return TaskDone.Done;
        }

        public Task DeleteQueueAysnc()
        {
            _channel.QueueDelete(QueueName);

            return TaskDone.Done;
        }

        /// <summary>
        /// Clears the queue.
        /// </summary>
        public Task ClearQueue()
        {
            _channel.QueuePurge(QueueName);
            return TaskDone.Done;
        }

        /// <summary>
        /// Adds a new message to the queue.
        /// </summary>
        /// <param name="message">Message to be added to the queue.</param>
        public Task AddQueueMessage(RabbitMessage message)
        {
            var startTime = DateTime.UtcNow;
            try
            {
                return Task.Factory.StartNew(() =>
                {
                    _channel.BasicPublish(string.Empty, QueueName, _commonMessageHeader, message.Body);
                });
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "AddQueueMessage");
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "AddQueueMessage");
            }

            return TaskDone.Done;
        }

        /// <summary>
        /// Gets a number of new messages from the queue.
        /// </summary>
        /// <param name="count">Number of messages to get from the queue.</param>
        public Task<IEnumerable<RabbitMessage>> GetQueueMessagesAsync(int count = -1)
        {
            var startTime = DateTime.UtcNow;
            try
            {
                return Task.Factory.StartNew(() =>
              {
                  var results = new List<RabbitMessage>();
                  var i = count;
                  while (i > 0 || i == -1)
                  {
                      var result = _channel.BasicGet(QueueName, false);
                      if (result == null)
                          break;
                      results.Add(new RabbitMessage(result.Body, result.DeliveryTag));
                      if (i != -1)
                          i--;
                  }
                  return results.AsEnumerable();
              });
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "AddQueueMessage");
                return null;
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "AddQueueMessage");
            }
        }

        /// <summary>
        /// Deletes a messages from the queue.
        /// </summary>
        /// <param name="message">A message to be deleted from the queue.</param>
        public async Task DeleteQueueMessage(RabbitMessage message)
        {
            var startTime = DateTime.UtcNow;
            try
            {
                await Task.Factory.StartNew(() =>
                {
                    _channel.BasicAck(message.DeliveryTag, false);
                });
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "DeleteMessage");
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "DeleteQueueMessage");
            }
        }


        private void CheckAlertSlowAccess(DateTime startOperation, string operation)
        {
            var timeSpan = DateTime.UtcNow - startOperation;
            //if (timeSpan > AzureQueueDefaultPolicies.QueueOperationTimeout)
            //{
            //    logger.Warn(ErrorCode.AzureQueue_13, "Slow access to Azure queue {0} for {1}, which took {2}.", QueueName, operation, timeSpan);
            //}
        }

        private void ReportErrorAndRethrow(Exception exc, string operation)
        {
            var errMsg = String.Format(
                "Error doing {0} for Azure storage queue {1} " + Environment.NewLine
                + "Exception = {2}", operation, QueueName, exc);
            //logger.Error(errorCode, errMsg, exc);
            throw new AggregateException(errMsg, exc);
        }

        #region Private Methods

        private IBasicProperties GetCommonMessageHeader()
        {
            var properties = this._channel.CreateBasicProperties();
            properties.Persistent = true;
            return properties;
        }
        #endregion
    }
}
