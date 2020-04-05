using Amazon.QLDB;
using Amazon.QLDB.Driver;
using Amazon.QLDB.Model;
using Amazon.QLDBSession;
using Amazon.Runtime;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Text;
using System.Threading;

namespace QLDBSample
{
    class Program
    {
        static void Main(string[] args)
        {
            string ledgerName = "my-ledger";
            var awsCredentials = new BasicAWSCredentials("sample", "key");

            Console.WriteLine($"Create the ledger '{ledgerName}'");

            AmazonQLDBClient qldbClient = new AmazonQLDBClient(awsCredentials, Amazon.RegionEndpoint.USEast2);
            CreateLedgerRequest createLedgerRequest = new CreateLedgerRequest
            {
                Name = ledgerName,
                PermissionsMode = PermissionsMode.ALLOW_ALL
            };
            qldbClient.CreateLedgerAsync(createLedgerRequest).GetAwaiter().GetResult();

            Console.WriteLine($"Waiting for ledger to be active");
            DescribeLedgerRequest describeLedgerRequest = new DescribeLedgerRequest
            {
                Name = ledgerName
            };
            while (true)
            {
                DescribeLedgerResponse describeLedgerResponse = qldbClient.DescribeLedgerAsync(describeLedgerRequest).GetAwaiter().GetResult();

                if (describeLedgerResponse.State.Equals(LedgerState.ACTIVE.Value))
                {
                    Console.WriteLine($"'{ ledgerName }' ledger created sucessfully.");
                    break;
                }
                Console.WriteLine($"Creating the '{ ledgerName }' ledger...");
                Thread.Sleep(1000);
            }

            AmazonQLDBSessionConfig amazonQldbSessionConfig = new AmazonQLDBSessionConfig();
            amazonQldbSessionConfig.RegionEndpoint = Amazon.RegionEndpoint.USEast2;
            Console.WriteLine($"Create the QLDB Driver");
            IQldbDriver driver = PooledQldbDriver.Builder().WithAWSCredentials(awsCredentials)
                .WithQLDBSessionConfig(amazonQldbSessionConfig)
                .WithLedger(ledgerName)
                .Build();

            string tableName = "MyTable1";
            var objsample = new SampleData() { FName = "Rangesh", LName = "Sripathi" };
            string jsonresult= JsonConvert.SerializeObject(objsample,Formatting.Indented);
            StringBuilder sb = new StringBuilder();
            using (StringWriter sw = new StringWriter(sb))
            using (JsonTextWriter writer = new JsonTextWriter(sw))
            {
                writer.QuoteChar = '\'';

                JsonSerializer ser = new JsonSerializer();
                ser.Serialize(writer, objsample);
            }

            Console.WriteLine(sb.ToString());


            using (IQldbSession qldbSession = driver.GetSession())
            {
                var text = $"INSERT INTO {tableName} VALUE" + sb;
                Console.WriteLine(text);
                // qldbSession.Execute will start a transaction and commit it.
                //IResult result = qldbSession.Execute($"CREATE TABLE {tableName}");
                qldbSession.Execute($"INSERT INTO {tableName} VALUE "+sb);
                Console.WriteLine($"Table '{tableName}' created");
            }
        }
    }

    public class SampleData
    {
        public string FName { get; set; }
        public string LName { get; set; }
    }
   
}
