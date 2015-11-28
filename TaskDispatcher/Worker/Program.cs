using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading;

class Worker
{
    /// <summary>
    ///     RabbitMq test, to demonstrate a work queue
    ///     A worker process will process one message at a time, while multiple worker processes are allowed.
    /// 
    ///     This console application will receive messages from RabbitMq and write the message one character 
    ///     per second to the console to demonstrate the ack mechanism.
    /// </summary>
    public static void Main()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            channel.QueueDeclare(queue: "task_queue",
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            // don't dispatch a new message to a worker until it has processed and acknowledged the previous one. 
            // instead, dispatch it to the next worker that is not still busy.
            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            Console.WriteLine(" [*] Waiting for messages.");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Received {0}", message);

                int i = 0;
                while (i < message.Length)
                {
                    Console.Write(message[i]);
                    i++;

                    Thread.Sleep(1000);
                }
                Console.WriteLine();

                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };

            channel.BasicConsume(queue: "task_queue",
                                    noAck: false,
                                    consumer: consumer);

            Console.ReadLine();
        }
    }
}