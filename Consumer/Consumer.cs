using Confluent.Kafka;
using Newtonsoft.Json;


var clientConfig = new ClientConfig();
clientConfig.BootstrapServers = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092";
clientConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
clientConfig.SaslMechanism = SaslMechanism.Plain;
clientConfig.SaslUsername = "SZTQHS22QZSEHBGW";
clientConfig.SaslPassword = "5fJ9UHRER225K4IA3S/Tf2s8AxcSLtxUCEtKEuGTDrUgbh+5RFt4TMOIk8CI7jqb";
clientConfig.SslCaLocation = "probe";

var consumerConfig = new ConsumerConfig (clientConfig);

consumerConfig.GroupId = "weather-consumer-group";
consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;

using var consumer = new ConsumerBuilder<string,string>(consumerConfig).Build();

consumer.Subscribe("weatherReport");

CancellationTokenSource token = new();

try
{
	while (true)
	{
		var response = consumer.Consume(token.Token);
		if(response.Message is not null)
		{
			var weather = JsonConvert.DeserializeObject<Weather>(response.Message.Value);
			Console.WriteLine($"State: {weather.State} \t Temperature: {weather.Temperature}F");
		}
	}
}
catch (Exception)
{

	throw;
}

finally 
{
	Console.WriteLine(nameof(consumer));
	consumer.Close();
}



public record Weather(string State, int Temperature);
