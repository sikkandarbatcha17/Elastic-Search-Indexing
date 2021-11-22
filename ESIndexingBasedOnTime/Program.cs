using Elasticsearch.Net;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using Nest;
using System.Diagnostics.Eventing.Reader;
namespace ESSample
{
    public class Etlog
    {
        public string Type { get; set; }

        public string Id { get; set; }
        public int Severity { get; set; }

        public string Source { get; set; }

        public long EventId { get; set; }

        public DateTime TimeGenerated { get; set; }

    }
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Connecting to Elastic-Search");

            var settings = new ConnectionSettings(new Uri("http://localhost:9200"));
            settings.DefaultIndex("windowslogs7");
            var client = new ElasticClient(settings);

            bool exists = client.Indices.Exists("windowslogs7").Exists;
            Dictionary<string, long?> map = new Dictionary<string, long?>();
            var startTime =  Convert.ToDateTime("15-11-1980");
            var endTime = DateTime.Now;
            string query=null;

            if(exists){
                var result = client.Search<Etlog>(s => s
                            .Size(0)
                            .Aggregations(a => a
                                .Terms("tags", t => t.Field(f => f.Type.Suffix("keyword")))));
                foreach (var bucket in result.Aggregations.Terms("tags").Buckets)
                {
                    string key = bucket.Key;
                    int value= (int)bucket.DocCount;
                    map.Add(key,value);               
                }

                var searchResponse = client.Search<Etlog>(s => s
                                .Size(1)
                                .Sort(so => so
                                .Field(fs => fs
                                .Field("timeGenerated")
                                .Order(SortOrder.Descending)
                        )));

                var myData = new List<Etlog>();
                foreach (var hit in searchResponse.Hits)
                {
                    myData.Add(hit.Source);
                }
                foreach(var item in myData){
                    startTime=item.TimeGenerated;
                }

                query = string.Format("*[System[TimeCreated[@SystemTime >= '{0}']]] and *[System[TimeCreated[@SystemTime <= '{1}']]]",
                startTime.ToUniversalTime().ToString("o"),
                endTime.ToUniversalTime().ToString("o"));
            }

            EventLog eventLog = new EventLog();
            EventLog[] remoteEventLogs;
            remoteEventLogs = EventLog.GetEventLogs("batcha-pt4528");
            Console.WriteLine("Number of logs on computer: " + remoteEventLogs.Length);
            Console.WriteLine("startTime: "+startTime);
            Console.WriteLine("endTime: "+endTime);
            
            long newIndexedDoc = 0;
            int diffInDocCount = 0;
            char ch= 'a';
            int totalDoc=0;
            List<object> evtlogs = new List<object>();
            foreach (EventLog log in remoteEventLogs)
             {
                newIndexedDoc=0;
                Console.WriteLine("Indexing " + log.Log + " event logs. . . ");
                eventLog.Log = log.Log;
                string ind = Convert.ToString(eventLog.Log).ToLower(); 

                long? value = map.TryGetValue(ind, out value) ? value : 0;
                totalDoc=(int)value;
                Console.WriteLine("Initial Document Count: " + totalDoc);
                EventLogEntryCollection myEventLogEntryCollection = eventLog.Entries;
                int myCount = myEventLogEntryCollection .Count;
                Console.WriteLine("The number of entries in "+ eventLog.Log +" = " +
                        myEventLogEntryCollection.Count);
                
                EventLogQuery eventsQuery = new EventLogQuery(eventLog.Log, PathType.LogName, query);
            
                try
                {
                EventLogReader logReader = new EventLogReader(eventsQuery);
            
                for (EventRecord eventdetail = logReader.ReadEvent(); eventdetail != null; eventdetail = logReader.ReadEvent())
                {
                    evtlogs.Add(new Etlog { Type = Convert.ToString(eventdetail.LogName).ToLower(), Id = Convert.ToString(totalDoc)+ch, Severity = (int)eventdetail.Level, Source = eventdetail.ProviderName, EventId = eventdetail.Id, TimeGenerated = (DateTime)eventdetail.TimeCreated });
                    totalDoc++;
                    diffInDocCount++;
                    newIndexedDoc++;

                }
                }
                catch (EventLogNotFoundException e)
                {
                    Console.WriteLine("Error while reading the event logs");
                    return;
                }
                Console.WriteLine(" Difference in Document Count: " + diffInDocCount);
                Console.WriteLine("Newly indexed Document: " + newIndexedDoc);
                 Console.WriteLine("Final Document Count: " + totalDoc+"\n");
                var bulkIndexResponse = client.Bulk(b => b.Index("windowslogs7").CreateMany(evtlogs));
                evtlogs.Clear();
                ch++;
            } 
            
        }
    }
}
