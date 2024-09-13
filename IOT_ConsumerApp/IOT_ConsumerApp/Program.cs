using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

public class Program
{
    private static readonly string RabbitMQHost = "localhost";
    private static readonly string RabbitMQQueue = "IOTDeviceDataQueue";
    private static readonly string InfluxDBUrl = "http://localhost:8086";
    private static readonly string InfluxDBToken = "xp6B_8fHA_FdSM3Gy5DpE3fN2bIdcCNKppboCEEomAwwrs1rgwcEYwoTnpIAiynfGsmr9x3OEuUStrerxRK34Q==";
    private static readonly string InfluxDBOrg = "Diya";
    private static readonly string InfluxDBBucket = "_monitoring";

    public static void Main(string[] args)
    {
        // Set up RabbitMQ connection and channel
        var factory = new ConnectionFactory() { HostName = RabbitMQHost };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        //channel.QueueDeclare(queue: RabbitMQQueue, durable: false, exclusive: false, autoDelete: false, arguments: null);

        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = System.Text.Encoding.UTF8.GetString(body);
            Console.WriteLine("Received message: {0}", message);

            // Send the message to InfluxDB
            await WriteToInfluxDB(message);
        };

        channel.BasicConsume(queue: RabbitMQQueue, autoAck: true, consumer: consumer);
        Console.WriteLine("Press [enter] to exit.");
        Console.ReadLine();
    }

    private static async Task WriteToInfluxDB(string message)
    {
        using var influxDBClient = InfluxDBClientFactory.Create(InfluxDBUrl, InfluxDBToken.ToCharArray());
        var writeApi = influxDBClient.GetWriteApiAsync();

        var point = PointData.Measurement("go_info")
            .Tag("source", "rabbitmq")
            .Field("value", message)
            .Timestamp(DateTime.UtcNow, WritePrecision.Ns);

        await writeApi.WritePointAsync(point, InfluxDBBucket, InfluxDBOrg);
        Console.WriteLine("Data written to InfluxDB.");
    }
}
