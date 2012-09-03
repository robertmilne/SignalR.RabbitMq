using System;
using RabbitMQ.Client;

namespace SignalR.RabbitMQ
{
    public static class DependencyResolverExtensions
    {
        public static IDependencyResolver UseRabbitMq(this IDependencyResolver resolver, ConnectionFactory connectionFactory, string exchangeName)
        {
            var bus = new Lazy<RabbitMqMessageBus>(() => new RabbitMqMessageBus(resolver, connectionFactory, exchangeName));

            resolver.Register(typeof(IMessageBus), () => bus.Value);

            return resolver;
        }
    }
}
