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

namespace QuantConnect.Lean.DataSource.Polygon;

/// <summary>
/// Defines the types of market data events that can be streamed via WebSocket,
/// covering various levels of aggregation and security types.
/// </summary>
public enum EventType
{
    /// <summary>
    /// No event type. Used when no subscription is active or the event is undefined.
    /// </summary>
    None = 0,

    /// <summary>
    /// Minute-level aggregated OHLC (Open, High, Low, Close) and volume data.
    /// </summary>
    AM = 1,

    /// <summary>
    /// Second-level aggregated OHLC (Open, High, Low, Close) and volume data.
    /// </summary>
    A = 2,

    /// <summary>
    /// Tick-by-tick trade data for supported securities.
    /// </summary>
    T = 3,

    /// <summary>
    /// National Best Bid and Offer (NBBO) quote data for supported securities.
    /// </summary>
    Q = 4,

    /// <summary>
    /// Real-time Fair Market Value (FMV) data for supported securities.
    /// </summary>
    FMV = 5,

    /// <summary>
    /// Real-time index value updates for specified indexes.
    /// </summary>
    V = 6
}
