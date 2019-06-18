using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Diagnostics;
using System.Threading;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
using Microsoft.Azure.CosmosDB.BulkExecutor.Graph;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.CosmosDB.BulkExecutor.Graph.Element;
using Newtonsoft.Json;

namespace WKGRAPH
{
    class Program
    {
        private static readonly string EndpointUrl = ConfigurationManager.AppSettings["EndpointUrl"];
        private static readonly string AuthorizationKey = ConfigurationManager.AppSettings["AuthorizationKey"];
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string CollectionName = ConfigurationManager.AppSettings["CollectionName"];
        private static readonly string pkPropertyName = ConfigurationManager.AppSettings["pkPropertyName"];

        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp
        };

        private DocumentClient client;

        public Program(DocumentClient client)
        {
            this.client = client;
        }

        static void Main(string[] args)
        {
            try
            {
                using (var client = new DocumentClient(
                    new Uri(EndpointUrl),
                    AuthorizationKey,
                    ConnectionPolicy))
                {
                    var program = new Program(client);
                    program.RunAsync().Wait();
                }
            }
            catch (AggregateException e)
            {
                Trace.TraceError("Caught AggregateException in Main, Inner Exception:\n" + e);
                Console.ReadKey();
            }

            Console.ReadKey();
        }

        private async Task RunAsync()
        {
            GraphSpec spec = JsonConvert.DeserializeObject<GraphSpec>(System.IO.File.ReadAllText("./spec.json"));

            var dataCollection = client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(DatabaseName, CollectionName)).Result.Resource;
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;

            GraphBulkExecutor graphbulkExecutor = new GraphBulkExecutor(client, dataCollection);
            graphbulkExecutor.InitializeAsync().Wait();

            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

            foreach (var vertexSpec in spec.vertices)
            {
                await RunBulkImportVerticesAsync(graphbulkExecutor, vertexSpec);
            }

            foreach (var edgeSpec in spec.edges)
            {
                int sourceCount = spec.vertices.FindLast(vs => vs.label.Equals(edgeSpec.from)).count;
                int targeCount = spec.vertices.FindLast(vs => vs.label.Equals(edgeSpec.to)).count;
                await RunBulkImportEdgesAsync(graphbulkExecutor, edgeSpec, sourceCount, targeCount);
            }

            Console.WriteLine("Finished importing events");
        }

        private async Task RunBulkImportEdgesAsync(
            GraphBulkExecutor graphbulkExecutor,
            EdgeSpec spec,
            int fromCount, 
            int toCount)
        {
            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            BulkImportResponse response = null;
          
            Random r = new Random();
            for (int i = 0; i < fromCount; i++)
            {
                List<GremlinEdge> edges = new List<GremlinEdge>();
                int edgeCount = r.Next(spec.maxCount - spec.minCount) + spec.minCount;
                for (int j = 0; j < edgeCount; j++)
                {
                    HashSet<int> neighbors = new HashSet<int>();
                    int destination = r.Next(toCount);
                    if (neighbors.Contains(destination))
                    {
                        j--;
                        continue;
                    }

                    neighbors.Add(destination);
                    string id = createEdgeId(i, spec.from, destination, spec.to);
                    GremlinEdge e = new GremlinEdge(
                        id, spec.label, createVertexId(spec.from, i), createVertexId(spec.to, destination),
                        spec.from, spec.to, createVertexId(spec.from, i), createVertexId(spec.to, destination));

                    for (int k = 0; k < spec.numberOfProperties; k++)
                    {
                        e.AddProperty("property_" + k, Guid.NewGuid().ToString());
                    }

                    edges.Add(e);
                }
                try
                {
                    response = await graphbulkExecutor.BulkImportAsync(
                            edges,
                            enableUpsert: true,
                            disableAutomaticIdGeneration: true,
                            maxConcurrencyPerPartitionKeyRange: null,
                            maxInMemorySortingBatchSize: null,
                            cancellationToken: token);
                }
                catch (DocumentClientException de)
                {
                    Trace.TraceError("Document client exception: {0}", de);
                }
                catch (Exception e)
                {
                    Trace.TraceError("Exception: {0}", e);
                }
            }
        }

        private async Task RunBulkImportVerticesAsync(GraphBulkExecutor graphbulkExecutor, VertexSpec vertexSpec)
        {
            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            BulkImportResponse response = null;

            List<GremlinVertex> vObjects = null;
            int currentCount = 0;
            while (currentCount < vertexSpec.count)
            {
                    
                vObjects = new List<GremlinVertex>();
                for (int i = 0; i < 100000 && currentCount < vertexSpec.count; i++)
                {
                    string id = createVertexId(vertexSpec.label, currentCount);


                    GremlinVertex v = new GremlinVertex(id, vertexSpec.label);
                    for (int j = 0; j < vertexSpec.numberOfProperties; j++)
                    {
                        v.AddProperty("property_" + j, Guid.NewGuid().ToString());
                    }

                    v.AddProperty(pkPropertyName, id);

                    currentCount += 1;
                    vObjects.Add(v);
                }

                try
                {
                    response = await graphbulkExecutor.BulkImportAsync(
                            vObjects,
                            enableUpsert: true,
                            disableAutomaticIdGeneration: true,
                            maxConcurrencyPerPartitionKeyRange: null,
                            maxInMemorySortingBatchSize: null,
                            cancellationToken: token);
                }
                catch (DocumentClientException de)
                {
                    Trace.TraceError("Document client exception: {0}", de);
                }
                catch (Exception e)
                {
                    Trace.TraceError("Exception: {0}", e);
                }
            }
        }


        private string createEdgeId(int i, string from, int destination, string to)
        {
            return createVertexId(from, i) + "->" + createVertexId(to, destination);
        }

        private static string createVertexId(string type, int currentCount)
        {
            return currentCount + "_" + type;
        }
    }
}

class GraphSpec
{
    public List<VertexSpec> vertices { get; set; }
    public List<EdgeSpec> edges { get; set; }
}

class EdgeSpec
{
    public string label { get; set; }
    public string from { get; set; }

    public string to { get; set; }

    public int minCount { get; set; }

    public int maxCount { get; set; }

    public int numberOfProperties { get; set; }
}

class VertexSpec
{
    public string label { get; set; }
    
    public int numberOfProperties { get; set; }

    public int count { get; set; }
}