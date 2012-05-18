using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Pitchfork.MessageStorage;

namespace Tester
{
    class Program
    {
        const int NumReaders = 24;
        const int NumWriters = 1;
        static readonly TimeSpan RunTime = TimeSpan.FromSeconds(10);

        static void Main(string[] args)
        {
            MessageStore<object> ms = new MessageStore<object>(capacity: 80000);
            ThreadPool.SetMinThreads(32, 32);

            Console.WriteLine("{0}-bit process", IntPtr.Size * 8);
            Console.WriteLine("{0} writer(s), {1} reader(s)", NumWriters, NumReaders);
            Console.WriteLine("Running for {0}..", RunTime);

            // queue writers
            for (int writerNum = 0; writerNum < NumWriters; writerNum++)
            {
                ThreadPool.QueueUserWorkItem(_ =>
                {
                    for (int i = 0; ; i++)
                    {
                        ms.Add("This is my awesome object!");
                    }
                }, null);
            }

            // queue readers
            long totalMessagesRecvd = 0;
            for (int readerNum = 0; readerNum < NumReaders; readerNum++)
            {
                ThreadPool.QueueUserWorkItem(_ =>
                {
                    ulong firstMessageIdToReceive = 0;
                    while (true)
                    {
                        var x = ms.GetMessages(firstMessageIdToReceive);
                        ulong firstMessageIdReceived = x.FirstMessageId;
                        firstMessageIdToReceive = firstMessageIdReceived + (ulong)x.Messages.Length;

                        if (x.Messages.Length > 0)
                        {
                            Interlocked.Add(ref totalMessagesRecvd, x.Messages.Length);
                        }
                        else
                        {
                            // back off if there is no more data
                            Thread.Sleep(10);
                        }

                    }
                }, null);
            }

            // let the program run for 10 seconds
            Thread.Sleep(RunTime);
            ulong totalMessagesWritten = ms.GetMessageCount();

            Console.WriteLine();
            Console.WriteLine("Throughput:");
            Console.WriteLine("{0:N0} total messages written", totalMessagesWritten);
            Console.WriteLine("  ({0:N0} messages per writer per second)", totalMessagesWritten / RunTime.TotalSeconds / NumWriters);
            Console.WriteLine("{0:N0} total messages received", totalMessagesRecvd);
            Console.WriteLine("  ({0:N0} messages per reader per second)", totalMessagesRecvd / RunTime.TotalSeconds / NumReaders);
        }
    }
}
