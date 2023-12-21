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
    /// Polygon Subscription Plan
    /// </summary>
    /// <remarks>Values most always be sorted in ascending order from most basic (cheaper) to most advanced (most expensive) plan</remarks>
    public enum PolygonSubscriptionPlan
    {
        /// <summary>
        /// Basic plan.
        /// This has access to aggregated bar historic data, but not streaming data.
        /// </summary>
        Basic,

        /// <summary>
        /// Starter plan.
        /// This has access to streaming aggregated bar data, but not tick data.
        /// </summary>
        Starter,

        /// <summary>
        /// Developer plan.
        /// This has access to streaming tick trade data, but not quotes.
        /// </summary>
        Developer,

        /// <summary>
        /// Advanced plan.
        /// This has access to streaming tick trade and quote data.
        /// </summary>
        Advanced
    }
}
