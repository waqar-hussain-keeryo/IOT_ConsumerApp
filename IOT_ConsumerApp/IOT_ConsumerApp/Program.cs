using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Globalization;

public class Program
{
    private static readonly string RabbitMQHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST");
    private static readonly string RabbitMQQueue = Environment.GetEnvironmentVariable("RABBITMQ_QUEUE");
    private static readonly string InfluxDBUrl = Environment.GetEnvironmentVariable("INFLUXDB_URL");
    private static readonly string InfluxDBToken = Environment.GetEnvironmentVariable("INFLUXDB_TOKEN");
    private static readonly string InfluxDBOrg = Environment.GetEnvironmentVariable("INFLUXDB_ORG");
    private static readonly string InfluxDBBucket = Environment.GetEnvironmentVariable("INFLUXDB_BUCKET");

    private static InfluxDBClient _influxDBClient;

    public static async Task Main(string[] args)
    {
        try
        {
            // Initialize InfluxDB client once for reuse
            _influxDBClient = InfluxDBClientFactory.Create(InfluxDBUrl, InfluxDBToken.ToCharArray());

            // Set up RabbitMQ connection and channel
            var factory = new ConnectionFactory() { HostName = RabbitMQHost };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += async (model, ea) => await ProcessMessage(channel, ea);
            channel.BasicConsume(queue: RabbitMQQueue, autoAck: false, consumer: consumer);

            Console.WriteLine("Listening for messages...");
            Console.WriteLine("Press [enter] to exit.");
            Console.ReadLine();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred during application execution: {ex.Message}");
        }
        finally
        {
            // Ensure the InfluxDB client is disposed of
            _influxDBClient?.Dispose(); // Changed from DisposeAsync to Dispose
        }
    }

    private static async Task ProcessMessage(IModel channel, BasicDeliverEventArgs ea)
    {
        var body = ea.Body.ToArray();
        var message = System.Text.Encoding.UTF8.GetString(body);
        Console.WriteLine("Received message: {0}", message);

        try
        {
            await WriteToInfluxDB(message);
            // Acknowledge the message after successful processing
            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing message: {ex.Message}");
            // Optionally: Handle or log the failure before Nack'ing
            channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
        }
    }

    private static async Task WriteToInfluxDB(string message)
    {
        // Deserialize the message into a list of lists of DataPoints
        List<List<DataPoint>> jsonData;
        try
        {
            jsonData = JsonConvert.DeserializeObject<List<List<DataPoint>>>(message);
        }
        catch (JsonException ex)
        {
            Console.WriteLine($"Failed to deserialize message: {ex.Message}");
            return; // Exit the method if deserialization fails
        }

        foreach (var dataPointList in jsonData)
        {
            var deviceId = GetValueFromDataPoint(dataPointList, "deviceid");
            var value = GetValueFromDataPoint(dataPointList, "value");
            var uom = GetValueFromDataPoint(dataPointList, "uom");
            var scheduledDate = GetValueFromDataPoint(dataPointList, "timestamp");

            // Validate values
            if (string.IsNullOrWhiteSpace(deviceId) || string.IsNullOrWhiteSpace(value) ||
                string.IsNullOrWhiteSpace(uom) || string.IsNullOrWhiteSpace(scheduledDate))
            {
                Console.WriteLine("One or more required fields are missing. Skipping.");
                continue;
            }

            // Parse the timestamp
            if (!DateTime.TryParseExact(scheduledDate, "MM-dd-yyyy/HH:mm:tt", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime timestamp))
            {
                Console.WriteLine($"Invalid scheduled date: {scheduledDate}");
                continue;
            }

            // Prepare point for InfluxDB based on UOM and write directly to InfluxDB
            try
            {
                var writeApi = _influxDBClient.GetWriteApiAsync();

                if (uom.Equals("Celsius", StringComparison.OrdinalIgnoreCase) &&
                    double.TryParse(value, out double temperature))
                {
                    var point = PointData
                        .Measurement("devicedata")
                        .Tag("deviceid", deviceId)
                        .Field("temperature", temperature)
                        .Timestamp(timestamp, WritePrecision.Ns);
                    await writeApi.WritePointAsync(point, InfluxDBBucket, InfluxDBOrg);
                }
                else if (uom.Equals("m/s", StringComparison.OrdinalIgnoreCase) &&
                         double.TryParse(value, out double windSpeed))
                {
                    var point = PointData
                        .Measurement("devicedata")
                        .Tag("deviceid", deviceId)
                        .Field("wind", windSpeed)
                        .Timestamp(timestamp, WritePrecision.Ns);
                    await writeApi.WritePointAsync(point, InfluxDBBucket, InfluxDBOrg);
                }
                else
                {
                    Console.WriteLine($"Invalid UOM or value. UOM: {uom}, Value: {value}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error writing to InfluxDB: {ex.Message}");
            }
        }
    }

    private static string GetValueFromDataPoint(List<DataPoint> dataPointList, string key)
    {
        return dataPointList.FirstOrDefault(dp => dp.Name.Equals(key, StringComparison.OrdinalIgnoreCase))?.Value;
    }

    public class DataPoint
    {
        public string Name { get; set; }
        public string Value { get; set; }
    }
}
