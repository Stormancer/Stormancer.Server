using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using Xunit.Abstractions;

namespace Stormancer.Raft.Tests
{
    internal class TestLoggerFactory : ILoggerFactory
    {
        private readonly ITestOutputHelper _outputHelper;

        public TestLoggerFactory(ITestOutputHelper outputHelper)
        {
            _outputHelper = outputHelper;
        }

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName)
        {
            return new TestLogger(categoryName,_outputHelper);
        }

        public void Dispose()
        {
        }
    }

    internal class TestLogger : ILogger
    {
        private readonly string _categoryName;
        private readonly ITestOutputHelper _output;

        public TestLogger(string categoryName,ITestOutputHelper output)
        {
            _categoryName = categoryName;
            _output = output;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => default;
      
        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        private object _lock = new object();
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_lock)
            {
                _output.WriteLine($"[{_categoryName,-64}] [{eventId.Id,-12}: {logLevel,-12}]\n{formatter(state, exception)}");
            }
         
        }
    }
}
