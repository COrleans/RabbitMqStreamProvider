using System;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Orleans.RabbitMqStreamProvider
{
    internal static class RabbitMessageQueueUtils
    {
        internal static IModel GetQueueChanel(string rabbitMqConnectionString)
        {
            if (rabbitMqConnectionString == null)
                throw new ArgumentNullException(nameof(rabbitMqConnectionString));

            var factory = new ConnectionFactory
            {
                Uri = rabbitMqConnectionString,
                AutomaticRecoveryEnabled = true,
                TaskScheduler = TaskScheduler.Current
            };
            var connection = factory.CreateConnection();
            return connection.CreateModel();
        }
    }
}
