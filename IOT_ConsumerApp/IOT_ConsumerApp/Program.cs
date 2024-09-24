using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Globalization;

public class Program
{
    private static readonly string RabbitMQHost = "localhost";
    private static readonly string RabbitMQQueue = "IOTDeviceDataQueue";

    private static readonly string InfluxDBUrl = "http://localhost:8086";
    private static readonly string InfluxDBToken = "3jaB3TSuGswRsp-t7uP5C_18_1xf60SdcVkGAr-gHxkjCieY8360LfIRSq4hk_HM8enVTbT4j_GdSgDolMwCzw==";
    private static readonly string InfluxDBOrg = "Diya";
    private static readonly string InfluxDBBucket = "IOTDB";

    private static ConcurrentQueue<PointData> pointsQueue = new ConcurrentQueue<PointData>();
    private static int batchSize = 100;
    private static bool isProcessing = false;

    public static void Main(string[] args)
    {
        // Set up RabbitMQ connection and channel
        var factory = new ConnectionFactory() { HostName = RabbitMQHost };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

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
        var influxDBClient = InfluxDBClientFactory.Create(InfluxDBUrl, InfluxDBToken.ToCharArray());

        // Deserialize the message into a list of lists of DataPoints
        var jsonData = JsonConvert.DeserializeObject<List<List<DataPoint>>>(message);

        foreach (var dataPointList in jsonData)
        {
            string deviceId = dataPointList.FirstOrDefault(dp => dp.Name == "deviceid")?.Value;
            string value = dataPointList.FirstOrDefault(dp => dp.Name == "value")?.Value;
            string uom = dataPointList.FirstOrDefault(dp => dp.Name == "uom")?.Value;
            string scheduledDate = dataPointList.FirstOrDefault(dp => dp.Name == "timestamp")?.Value;

            if (!DateTime.TryParseExact(scheduledDate, "MM-dd-yyyy/HH:mm:tt", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime timestamp))
            {
                Console.WriteLine($"Invalid scheduled date: {scheduledDate}");
                continue;
            }

            if (uom == "Celsius" && double.TryParse(value, out double temperature))
            {
                pointsQueue.Enqueue(PointData
                    .Measurement("devicedata")
                    .Tag("deviceId", deviceId)
                    .Field("temperature", temperature)
                    .Timestamp(timestamp, WritePrecision.Ns));
            }
            else if (uom == "m/s" && double.TryParse(value, out double windSpeed))
            {
                pointsQueue.Enqueue(PointData
                    .Measurement("devicedata")
                    .Tag("deviceId", deviceId)
                    .Field("wind", windSpeed)
                    .Timestamp(timestamp, WritePrecision.Ns));
            }
            else
            {
                Console.WriteLine("Invalid UOM or value.");
            }
        }

        // Trigger batch write if processing is not already in progress
        if (!isProcessing)
        {
            isProcessing = true;
            _ = Task.Run(() => FlushPointsToInfluxDB(influxDBClient)); // Fire and forget
        }
    }

    private static async Task FlushPointsToInfluxDB(InfluxDBClient influxDBClient)
    {
        while (true)
        {
            if (pointsQueue.IsEmpty)
            {
                isProcessing = false;
                return;
            }

            var writeApi = influxDBClient.GetWriteApiAsync();
            var batchPoints = new List<PointData>();

            // Collect points for the batch
            while (pointsQueue.TryDequeue(out var point) && batchPoints.Count < batchSize)
            {
                batchPoints.Add(point);
            }

            if (batchPoints.Count > 0)
            {
                await writeApi.WritePointsAsync(batchPoints, InfluxDBBucket, InfluxDBOrg);
                Console.WriteLine($"Batch write to InfluxDB completed. Total points: {batchPoints.Count}.");
            }
        }
    }

    public class DataPoint
    {
        public string Name { get; set; }
        public string Value { get; set; }
    }
}
