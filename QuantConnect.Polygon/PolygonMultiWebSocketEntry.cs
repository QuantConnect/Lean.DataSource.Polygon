/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Helper class for <see cref="PolygonMultiWebSocketSubscriptionManager"/>
    /// </summary>
    public class PolygonMultiWebSocketEntry
    {
        public class Subscription
        {
            public Symbol Symbol { get; set; }
            public TickType TickType { get; set; }

            public Subscription(Symbol symbol, TickType tickType)
            {
                Symbol = symbol;
                TickType = tickType;
            }

            public override bool Equals(object obj)
            {
                if (obj is not Subscription other)
                {
                    return false;
                }
                return Symbol == other.Symbol && TickType == other.TickType;
            }

            public override int GetHashCode()
            {
                return Symbol.GetHashCode() ^ TickType.GetHashCode();
            }
        }

        private readonly HashSet<Subscription> _subscriptions;
        private readonly object _lock = new();

        /// <summary>
        /// Gets the web socket instance
        /// </summary>
        public PolygonWebSocketClientWrapper WebSocket { get; }

        /// <summary>
        /// Gets the number of symbols subscribed
        /// </summary>
        public int SubscriptionCount
        {
            get
            {
                lock (_lock)
                {
                    return _subscriptions.Count;
                }
            }
        }

        /// <summary>
        /// Returns whether the entry a subscription for the specified symbol and tick type
        /// </summary>
        /// <param name="symbol">The subscribed symbol</param>
        /// <param name="tickType">The subscribed tick type</param>
        /// <returns></returns>
        public bool Contains(Symbol symbol, TickType tickType)
        {
            lock (_lock)
            {
                return _subscriptions.Contains(new Subscription(symbol, tickType));
            }
        }

        /// <summary>
        /// Returns the list of subscriptions
        /// </summary>
        /// <returns></returns>
        public IReadOnlyCollection<Subscription> Subscriptions
        {
            get
            {
                lock (_lock)
                {
                    return _subscriptions.ToList();
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonMultiWebSocketEntry"/> class
        /// </summary>
        /// <param name="symbolWeights">A dictionary of symbol weights</param>
        /// <param name="webSocket">The web socket instance</param>
        public PolygonMultiWebSocketEntry(PolygonWebSocketClientWrapper webSocket)
        {
            _subscriptions = new();
            WebSocket = webSocket;
        }

        /// <summary>
        /// Adds a subscription to the entry
        /// </summary>
        /// <param name="symbol">The symbol to add</param>
        /// <param name="tickType">The tick type of the subscription</param>
        public void AddSubscription(Symbol symbol, TickType tickType)
        {
            lock (_lock)
            {
                _subscriptions.Add(new Subscription(symbol, tickType));
            }
        }

        /// <summary>
        /// Removes a subscription from the entry
        /// </summary>
        /// <param name="symbol">The symbol to remove</param>
        /// <param name="tickType">The tick type of the subscription</param>
        public void RemoveSubscription(Symbol symbol, TickType tickType)
        {
            lock (_lock)
            {
                _subscriptions.Remove(new Subscription(symbol, tickType));
            }
        }
    }
}
