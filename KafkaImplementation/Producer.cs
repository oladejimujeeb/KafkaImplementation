using Confluent.Kafka;
using Newtonsoft.Json;

var clientConfig = new ClientConfig();
clientConfig.BootstrapServers = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092";
clientConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
clientConfig.SaslMechanism = SaslMechanism.Plain;
clientConfig.SaslUsername = "SZTQHS22QZSEHBGW";
clientConfig.SaslPassword = "5fJ9UHRER225K4IA3S/Tf2s8AxcSLtxUCEtKEuGTDrUgbh+5RFt4TMOIk8CI7jqb";
clientConfig.SslCaLocation = "probe";

using var producer = new ProducerBuilder<string, string>(clientConfig).Build();
 
try
{
	string? state;
	while ((state= Console.ReadLine()) != null)
	{
        var response = await producer.ProduceAsync("weatherReport", new Message<string, string> { Key = Guid.NewGuid().ToString().Replace("/", "-").Substring(5),
									Value = JsonConvert.SerializeObject(new Weather(state, 70)) });
		Console.WriteLine(response.Value);
    }
}
catch (ProduceException<string, string> ex)
{
	Console.WriteLine(ex.Message);
}
finally
{
	Console.WriteLine("Done!");
}

public record Weather(string State, int Temperature);
