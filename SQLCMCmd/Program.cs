using CommandLine;
using NLog;
using SQLCMCore;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SQLCMCmd
{
	class Program
	{
		private static Logger logger = LogManager.GetCurrentClassLogger();
        private static CancellationTokenSource source;


        static void Main(string[] args)
		{
			AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(GenericErrorHandler);
			GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;

			System.Reflection.Assembly assembly = System.Reflection.Assembly.GetExecutingAssembly();
			FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
			string version = fvi.FileMajorPart.ToString() + "." + fvi.FileMinorPart.ToString() + "." + fvi.FileBuildPart.ToString();
			string name = assembly.FullName;
			logger.Info(name + " " + version);


			try
			{
				CommandLine.Parser.Default.ParseArguments<Options>(args)
					.WithParsed<Options>(opts => Run(opts));
			}
			catch (Exception e)
			{
				logger.Error(e);
			}

#if DEBUG
			Console.WriteLine("Press any key to close");
			Console.Read();
#endif

		}

		private static void Run(Options options)
		{
            Collector c = new Collector(options.ServerName, options.DatabaseName);
            c.Path = options.Path;

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                e.Cancel = true;
                logger.Info("Received shutdown signal...");
                source.CancelAfter(TimeSpan.FromSeconds(10)); // give a 10 seconds cancellation grace period 
                c.Stop();
            };

            Task t = processCollector(c);
            t.Wait();
            logger.Info("Collector stopped.");
        }

		static void GenericErrorHandler(object sender, UnhandledExceptionEventArgs e)
		{
			try
			{
				logger.Error(e.ToString());
			}
			finally
			{
				Console.WriteLine("Caught unhandled exception...");

			}
		}

        public static async Task processCollector(Collector collector)
        {
            source = new CancellationTokenSource();
            source.Token.Register(CancelNotification);
            var completionSource = new TaskCompletionSource<object>();
            source.Token.Register(() => completionSource.TrySetCanceled());
            var task = Task.Factory.StartNew(() => collector.Run(), source.Token);
            await Task.WhenAny(task, completionSource.Task);
        }


        public static void CancelNotification()
        {
            logger.Info("Shutdown complete.");
        }
    }

	class Options
	{
		[Option('S', "Server", Required = true, HelpText = "Name of the SQLServer instance that contains the repository database")]
		public string ServerName { get; set; }

		[Option('D', "Database", Required = true, HelpText = "Name of the repository database")]
		public string DatabaseName { get; set; }

		[Option('P', "Path", Required = true, HelpText = "Parent path of the servers tree on the repository database")]
		public string Path { get; set; }
	}
}
