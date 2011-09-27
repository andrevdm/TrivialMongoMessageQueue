using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nancy;
using Nancy.Hosting.Self;

namespace TmMqStats
{
    class Program
    {
        static void Main( string[] args )
        {
            var host = new NancyHost( new Uri( "http://localhost:12345" ) );
            host.Start();

            Console.ReadKey();
            host.Stop();
        }
    }

    public class MainModule : NancyModule
    {
        public MainModule()
        {
            Get["/"] = x => { return "Hello World"; };
            Get["/test"] = x => { return "test"; };
        }
    }
}
