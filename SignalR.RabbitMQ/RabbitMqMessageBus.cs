using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SignalR.RabbitMQ
{
    public class RabbitMqMessageBus : IMessageBus, IIdGenerator<long>
    {
        private readonly InProcessMessageBus<long> _bus;
        private readonly ConnectionFactory _connectionFactory;
        private readonly string _exchangeName;
        private IModel _channel;
        private int _resource = 0;

        public RabbitMqMessageBus(IDependencyResolver resolver, ConnectionFactory connectionFactory, string exchangeName)
        {
            _bus = new InProcessMessageBus<long>(resolver, this);
            _connectionFactory = connectionFactory;
            _exchangeName = exchangeName;

            EnsureConnection();
        }

        public Task<MessageResult> GetMessages(IEnumerable<string> eventKeys, string id, CancellationToken timeoutToken)
        {
            return _bus.GetMessages(eventKeys, id, timeoutToken);
        }

        public Task Send(string connectionId, string eventKey, object value)
        {
            var message = new RabbitMqMessageWrapper(connectionId, eventKey, value);
            return Task.Factory.StartNew(SendMessage, message);
        }

        public long ConvertFromString(string value)
        {
            return Int64.Parse(value, CultureInfo.InvariantCulture);
        }

        public string ConvertToString(long value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }

        public long GetNext()
        {
            return DateTime.Now.Ticks;
        }

        private void SendMessage(object state)
        {
            var message = (RabbitMqMessageWrapper) state;
            byte[] payload = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));

            if (_channel != null && _channel.IsOpen)
                _channel.BasicPublish(_exchangeName, message.EventKey, null, payload);
        }

        private void EnsureConnection()
        {
            if (1 == Interlocked.Exchange(ref _resource, 1))
            {
                return;
            }

            ThreadPool.QueueUserWorkItem(_ =>
            {
                while (true)
                {
                    try
                    {
                        using (var connection = _connectionFactory.CreateConnection())
                        {
                            _channel = connection.CreateModel();
                            _channel.ExchangeDeclare(_exchangeName, "topic", true);

                            var queue = _channel.QueueDeclare("", false, false, true, null);
                            _channel.QueueBind(queue.QueueName, _exchangeName, "#");

                            var consumer = new QueueingBasicConsumer(_channel);
                            _channel.BasicConsume(queue.QueueName, false, consumer);

                            while (_channel.IsOpen)
                            {
                                var ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                                _channel.BasicAck(ea.DeliveryTag, false);

                                string json = Encoding.UTF8.GetString(ea.Body);

                                var message = JsonConvert.DeserializeObject<RabbitMqMessageWrapper>(json);

                                _bus.Send(message.ConnectionIdentifier, message.EventKey, message.Value);
                            }
                        }
                    }
                    catch
                    {
                        // log this or error callback?
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(5));
                }
            });
        }
    }
}