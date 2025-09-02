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
 *
*/

namespace QuantConnect.Lean.DataSource.Polygon;

/// <summary>
/// Exception thrown when a tick type is not supported for the current Polygon license.
/// </summary>
public class UnsupportedTickTypeForLicenseException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="UnsupportedTickTypeForLicenseException"/> class
    /// with the specified tick type and license type.
    /// </summary>
    /// <param name="tickType">The tick type that was requested (e.g., Trade, Quote).</param>
    /// <param name="licenseType">The license type of the current Polygon subscription (Individual or Business).</param>
    public UnsupportedTickTypeForLicenseException(string tickType, LicenseType licenseType)
        : base($"TickType '{tickType}' is not supported for license type '{licenseType}'.")
    {
    }
}
